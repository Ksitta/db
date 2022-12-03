from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import sm_manager
from table.table import Column
from typing import List, Dict, Tuple
from config import *
from operators.operators import *
from common.disjointset import DisjointSet

class DBVisitor(SQLVisitor):
    def visitProgram(self, ctx: SQLParser.ProgramContext):
        stmts = ctx.statement()
        if(stmts is None):
            return
        if(len(stmts) == 1):
            return stmts[0].accept(self)
        for each in stmts:
            each.accept(self)
        return
    
    def visitStatement(self, ctx: SQLParser.StatementContext):
        if (ctx.db_statement()):
            return ctx.db_statement().accept(self)
        if (ctx.io_statement()):
            return ctx.io_statement().accept(self)
        if (ctx.table_statement()):
            return ctx.table_statement().accept(self)
        if (ctx.alter_statement()):
            return ctx.alter_statement().accept(self)
        if (ctx.Annotation()):
            return ctx.Annotation().accept(self)
        return None
    
    def visitCreate_db(self, ctx: SQLParser.Create_dbContext):
        return sm_manager.create_db(str(ctx.Identifier()))

    def visitDrop_db(self, ctx: SQLParser.Drop_dbContext):
        return sm_manager.drop_db(str(ctx.Identifier()))

    def visitShow_dbs(self, ctx: SQLParser.Show_dbsContext):
        dbs = sm_manager.show_dbs()
        return dbs

    def visitUse_db(self, ctx: SQLParser.Use_dbContext):
        return sm_manager.open_db(str(ctx.Identifier()))

    def visitShow_tables(self, ctx: SQLParser.Show_tablesContext):
        tables = sm_manager.show_tables()
        return tables
        
    def visitShow_indexes(self, ctx: SQLParser.Show_indexesContext):
        return sm_manager.show_indexes()

    def visitLoad_data(self, ctx: SQLParser.Load_dataContext):
        file_name: str = str(ctx.String())[1:-1]
        table_name: str = str(ctx.Identifier())
        sm_manager.load(table_name, file_name)
    
    def visitDump_data(self, ctx: SQLParser.Dump_dataContext):
        file_name = str(ctx.String())[1:-1]
        table_name = str(ctx.Identifier())
        sm_manager.dump(table_name, file_name)

    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        self._attrs: list = list()
        self._pk: list = list()
        self._fk: list = list()
        ctx.field_list().accept(self)
        sm_manager.create_table(
            str(ctx.Identifier()), self._attrs, self._pk, self._fk)

    def visitDrop_table(self, ctx: SQLParser.Drop_tableContext):
        sm_manager.drop_table(str(ctx.Identifier()))

    def visitDescribe_table(self, ctx: SQLParser.Describe_tableContext):
        return sm_manager.describe_table(str(ctx.Identifier()))

    def visitInsert_into_table(self, ctx: SQLParser.Insert_into_tableContext):
        table_name = str(ctx.Identifier())
        values = ctx.value_lists().accept(self)
        sm_manager.insert(table_name, values)

    def visitDelete_from_table(self, ctx: SQLParser.Delete_from_tableContext):
        table_name = str(ctx.Identifier())
        self._table_scan = {}
        self._table_scan[table_name] = TableScanNode(sm_manager.get_table(table_name))
        self._table_names = [table_name]
        ctx.where_and_clause().accept(self)
        
        node = self._table_scan[table_name]
        records: RecordList = node.process()
        sm_manager.delete(table_name, records)

        
    def visitUpdate_table(self, ctx: SQLParser.Update_tableContext):
        table_name = str(ctx.Identifier())
        self._table_names = [table_name]
        self._table_scan = {}
        self._table_scan[table_name] = TableScanNode(sm_manager.get_table(table_name))
        ctx.where_and_clause().accept(self)
        set_clause = ctx.set_clause().accept(self)
        node = self._table_scan[table_name]
        records: RecordList = node.process()
        sm_manager.update(table_name, records, set_clause)
        
    def visitSelect_table(self, ctx: SQLParser.Select_tableContext):
        selectors: List[Col] = ctx.selectors().accept(self)
        idents = ctx.identifiers().accept(self)
        self._table_names = idents
        idents_set = set(idents)
        if len(idents_set) != len(idents):
            raise Exception("Duplicate table name")
        
        djset = DisjointSet(idents_set)

        for each in selectors:
            if(each.table_name is None and each.col_name != "*"):
                each.table_name = sm_manager.get_table_name(each.col_name, idents)

        # add scan node
        self._table_scan: Dict[str, TableScanNode] = {}
        for each in idents:
            self._table_scan[each] = TableScanNode(sm_manager.get_table(each))

        self._table_join: Dict[str, JoinCondition] = {}

        if(ctx.where_and_clause()):
            ctx.where_and_clause().accept(self)

        for each in self._table_join:
            [t1, t2] = each.split(".")
            t1 = djset.find(t1)
            t2 = djset.find(t2)
            djset.union(t1, t2)
            cond = self._table_join[each]
            dst = djset.find(t1)
            self._table_scan[dst] = JoinNode(self._table_scan[t1], self._table_scan[t2], cond)

        for each in idents[1:]:
            if (djset.is_connect(idents[0], each)):
                continue
            raise Exception("Not all tables are joined")

        node = self._table_scan[djset.find(idents[0])]

        # add project node
        if(len(selectors) != 0):
            node = ProjectNode(node, selectors)

        return node.process()
        
    def visitAlter_add_index(self, ctx: SQLParser.Alter_add_indexContext):
        table_name = str(ctx.Identifier())
        idents = ctx.identifiers().accept(self)
        sm_manager.create_index(table_name, idents)

    def visitAlter_drop_index(self, ctx: SQLParser.Alter_drop_indexContext):
        table_name = str(ctx.Identifier())
        idents = ctx.identifiers().accept(self)
        sm_manager.drop_index(table_name, idents)

    def visitAlter_table_drop_pk(self, ctx: SQLParser.Alter_table_drop_pkContext):
        table_name = str(ctx.Identifier(0))
        sm_manager.drop_pk(table_name)

    def visitAlter_table_drop_foreign_key(self, ctx: SQLParser.Alter_table_drop_foreign_keyContext):
        table_name = str(ctx.Identifier())
        idents = ctx.Identifier()
        sm_manager.drop_fk(table_name, idents)

    def visitAlter_table_add_pk(self, ctx: SQLParser.Alter_table_add_pkContext):
        table_name: str = str(ctx.Identifier(0))
        idents: List[str] = ctx.identifiers().accept(self)
        sm_manager.add_pk(table_name, idents)
        
    def visitAlter_table_add_foreign_key(self, ctx: SQLParser.Alter_table_add_foreign_keyContext):
        table_name = str(ctx.Identifier(0))
        fk_name = str(ctx.Identifier(1))
        target_table = str(ctx.Identifier(2))
        local_idents = ctx.identifiers(0).accept(self)
        target_idents = ctx.identifiers(1).accept(self)
        sm_manager.add_fk(table_name, fk_name, target_table, local_idents, target_idents)

    def visitAlter_table_add_unique(self, ctx: SQLParser.Alter_table_add_uniqueContext):
        table_name = str(ctx.Identifier(0))
        idents: List[str] = ctx.identifiers().accept(self)
        raise NotImplementedError()
        
    def visitField_list(self, ctx: SQLParser.Field_listContext):
        for each in ctx.field():
            each.accept(self)

    def visitField(self, ctx: SQLParser.FieldContext):
        ctx.accept(self)
    
    
    def visitNormal_field(self, ctx: SQLParser.Normal_fieldContext):
        (type_t, type_len) = ctx.type_().accept(self)
        ident: str = str(ctx.Identifier())
        nullable: bool = ctx.Null() is not None
        default_val = ctx.value()
        if default_val is not None:
            if type_t == TYPE_INT:
                default_val: int = int(default_val)
            elif type_t == TYPE_FLOAT:
                default_val: float = float(default_val)
            elif type_t == TYPE_STR:
                default_val: str = str(default_val)
        self._attrs.append(
            Column(ident, type_t, type_len, nullable, default_val))

    def visitPrimary_key_field(self, ctx: SQLParser.Primary_key_fieldContext):
        self._pk: List[str] = ctx.identifiers().accept(self)

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        ident = ctx.Identifier(0)
        target_table = ctx.Identifier(1)
        if (target_table is None):
            target_table = str(ident)
            ident = "_fk_" + str(len(self._fk))
        else:
            ident = str(ident)
            target_table = str(target_table)

        fk = {}
        fk["foreign_key_name"] = ident
        fk["foreign_key_name_length"] = len(ident)

        fk["target_table_name"] = target_table
        fk["target_table_name_length"] = len(target_table)

        local_idents = ctx.identifiers(0).accept(self)
        target_idents = ctx.identifiers(1).accept(self)
        if (len(local_idents) != len(target_idents)):
            raise Exception(
                "Foreign key error: local idents and target idents are not equal.")

        fk["foreign_key_size"] = len(local_idents)
        fk["local_idents"] = local_idents
        fk["target_idents"] = target_idents
        self._fk.append(fk)

        
    def visitType_(self, ctx: SQLParser.Type_Context):
        text = str(ctx.getChild(0))
        if (text == 'INT'):
            return (TYPE_INT, 4)
        if (text == 'FLOAT'):
            return (TYPE_FLOAT, 8)
        if (text == 'VARCHAR'):
            return (TYPE_STR, int(ctx.Integer().getText()))
        
    def visitValue_lists(self, ctx: SQLParser.Value_listsContext):
        value_lists = [each.accept(self) for each in ctx.value_list()]
        return value_lists

    def visitValue_list(self, ctx: SQLParser.Value_listContext):
        values = [each.accept(self) for each in ctx.value()]
        return values

    def visitValue(self, ctx: SQLParser.ValueContext):
        if ctx.Integer() is not None:
            return int(ctx.Integer().getText())
        if ctx.String() is not None:
            return str(ctx.String().getText())[1:-1:]
        if ctx.Float() is not None:
            return float(ctx.Float().getText())
        return None

    def visitWhere_and_clause(self, ctx: SQLParser.Where_and_clauseContext):
        for each in ctx.where_clause():
            each.accept(self)

    def visitWhere_operator_expression(self, ctx: SQLParser.Where_operator_expressionContext):
        col: Col = ctx.column().accept(self)
        op = ctx.operator_().accept(self)
        exp = ctx.expression().accept(self)
        if (type(exp) is Col):
            cond = JoinCondition(col, exp)
            self._table_join[col.table_name + '.' + exp.table_name] = cond
            return
        if (col.table_name is None):
            col.table_name = sm_manager.get_table_name(col.col_name, self._table_names)
        old_node = self._table_scan[col.table_name]
        cond = AlgebraCondition(op, old_node.get_column_idx(col), exp)
        self._table_scan[col.table_name].add_condition(cond)

    def visitWhere_operator_select(self, ctx: SQLParser.Where_operator_selectContext):
        col = ctx.column().accept(self)
        op = ctx.operator_().accept(self)
        raw_scan = self._table_scan
        raw_tables = self._table_names
        raw_table_join = self._table_join
        
        records: RecordList = ctx.select_table().accept(self)
        if (len(records.records) != 1):
            raise Exception("Subquery must have only one record.")
        if (len(records.columns) != 1):
            raise Exception("Subquery must have only one column.")

        self._table_scan = raw_scan
        self._table_names = raw_tables
        self._table_join = raw_table_join

        if (col.table_name is None):
            col.table_name = sm_manager.get_table_name(col.col_name, self._table_names)
        old_node = self._table_scan[col.table_name]
        cond = AlgebraCondition(op, old_node.get_column_idx(col), records.records[0].data[0])
        self._table_scan[col.table_name].add_condition(cond)

    def visitWhere_null(self, ctx: SQLParser.Where_nullContext):
        col = ctx.column().accept(self)
        # if(ctx.Null() is not None):
        #     cond = AlgebraCondition(CompOp.NE, None)
        # else:
        #     cond = AlgebraCondition(CompOp.EQ, None)

    def visitWhere_in_list(self, ctx: SQLParser.Where_in_listContext):
        col = ctx.column().accept(self)
        values = ctx.value_list().accept(self)

        if (col.table_name is None):
            col.table_name = sm_manager.get_table_name(col.col_name, self._table_names)
        old_node = self._table_scan[col.table_name]
        cond = InListCondition(old_node.get_column_idx(col), set(values))
        self._table_scan[col.table_name].add_condition(cond)

    def visitWhere_in_select(self, ctx: SQLParser.Where_in_selectContext):
        col = ctx.column().accept(self)
        raw_scan = self._table_scan
        raw_tables = self._table_names
        raw_table_join = self._table_join
        
        records: RecordList = ctx.select_table().accept(self)
        if (len(records.columns) != 1):
            raise Exception("Subquery must have only one column.")

        self._table_scan = raw_scan
        self._table_names = raw_tables
        self._table_join = raw_table_join

        if (col.table_name is None):
            col.table_name = sm_manager.get_table_name(col.col_name, self._table_names)
        old_node = self._table_scan[col.table_name]
        values = [each.data[0] for each in records.records]
        cond = InListCondition(old_node.get_column_idx(col), set(values))
        self._table_scan[col.table_name].add_condition(cond)

    def visitWhere_like_string(self, ctx: SQLParser.Where_like_stringContext):
        col = ctx.column().accept(self)
        pass

    def visitColumn(self, ctx: SQLParser.ColumnContext) -> Col:
        table_name = str(ctx.Identifier(0))
        col_name = ctx.Identifier(1)
        if col_name is None:
            col_name = table_name
            table_name = None
        else:
            col_name = str(col_name)
        return Col(col_name, table_name)

    def visitSet_clause(self, ctx: SQLParser.Set_clauseContext):
        idents = [str(each) for each in ctx.Identifier()]
        values = [each.accept(self) for each in ctx.value()]
        return list(zip(idents, values))

    def visitSelectors(self, ctx: SQLParser.SelectorsContext):
        if(ctx.selector() is not None):
            return [each.accept(self) for each in ctx.selector()]
        return []

    def visitSelector(self, ctx: SQLParser.SelectorContext):
        if(ctx.column() is not None):
            col: Col = ctx.column().accept(self)
            if(ctx.aggregator() is not None):
                col.aggregator = ctx.aggregator().accept(self)
            return col
        return Col("*", None, Aggregator.COUNT)
    
    def visitIdentifiers(self, ctx: SQLParser.IdentifiersContext):
        idents: List[str] = [str(each) for each in ctx.Identifier()]
        return idents
    
    def visitOperator_(self, ctx: SQLParser.Operator_Context):
        if (ctx.EqualOrAssign() is not None):
            return CompOp.EQ
        if (ctx.Less() is not None):
            return CompOp.LT
        if (ctx.LessEqual() is not None):
            return CompOp.LE
        if (ctx.Greater() is not None):
            return CompOp.GT
        if (ctx.GreaterEqual() is not None):
            return CompOp.GE
        if (ctx.NotEqual() is not None):
            return CompOp.NE
    
    def visitAggregator(self, ctx: SQLParser.AggregatorContext):
        if (ctx.Count() is not None):
            return Aggregator.COUNT
        if (ctx.Sum() is not None):
            return Aggregator.SUM
        if (ctx.Average() is not None):
            return Aggregator.AVG
        if (ctx.Min() is not None):
            return Aggregator.MIN
        if (ctx.Max() is not None):
            return Aggregator.MAX
    