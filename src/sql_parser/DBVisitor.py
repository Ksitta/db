from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import sm_manager
from table.table import Column
from typing import List, Dict
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
        return super().visitShow_indexes(ctx)

    def visitLoad_data(self, ctx: SQLParser.Load_dataContext):
        return super().visitLoad_data(ctx)
    
    def visitDump_data(self, ctx: SQLParser.Dump_dataContext):
        return super().visitDump_data(ctx)

    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        self._attrs: list = list()
        self._pk: list = list()
        self._fk: dict = dict()
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
        
        
    def visitUpdate_table(self, ctx: SQLParser.Update_tableContext):
        table_name = str(ctx.Identifier())

    def visitSelect_table(self, ctx: SQLParser.Select_tableContext):
        selectors: List[Col] = ctx.selectors().accept(self)
        idents = ctx.identifiers().accept(self)
        where_clause = None
        if(ctx.where_and_clause()):
            where_clause = ctx.where_and_clause().accept(self)

        idents_set = set(idents)
        if len(idents_set) != len(idents):
            raise Exception("Duplicate table name")
        
        djset = DisjointSet(idents_set)

        for each in selectors:
            if(each.table_name is None):
                each.table_name = sm_manager.get_table_name(each.col_name, idents)

        # add scan node
        table_scan: Dict[str, OperatorBase] = {}
        for each in idents:
            table_scan[each] = TableScanNode(sm_manager.get_table(each))
                
        for each in idents[1:]:
            if (djset.is_connect(idents[0], each)):
                continue
            raise Exception("Not all tables are joined")

        node = table_scan[djset.find(idents[0])]

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
        pass

    def visitAlter_table_drop_foreign_key(self, ctx: SQLParser.Alter_table_drop_foreign_keyContext):
        pass

    def visitAlter_table_add_pk(self, ctx: SQLParser.Alter_table_add_pkContext):
        table_name: str = str(ctx.Identifier(0))
        idents: List[str] = ctx.identifiers().accept(self)
        pass
        
    def visitAlter_table_add_foreign_key(self, ctx: SQLParser.Alter_table_add_foreign_keyContext):
        table_name = str(ctx.Identifier(0))
        fk_name = str(ctx.Identifier(1))
        target_table = str(ctx.Identifier(2))
        local_idents = ctx.identifiers(0).accept(self)
        target_idents = ctx.identifiers(1).accept(self)
        pass

    def visitAlter_table_add_unique(self, ctx: SQLParser.Alter_table_add_uniqueContext):
        table_name = str(ctx.Identifier(0))
        idents: List[str] = ctx.identifiers().accept(self)
        pass
        
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
        self._fk: dict = {}
        self._fk["constraint_name"] = str(ctx.Identifier(0))
        self._fk["target_table"] = str(ctx.Identifier(1))
        self._fk["local_idents"] = ctx.identifiers(0).accept(self)
        self._fk["target_idents"] = ctx.identifiers(1).accept(self)
        if (len(self._fk["local_idents"]) != len(self._fk["target_idents"])):
            raise Exception(
                "Foreign key error: local idents and target idents are not equal.")

        
    def visitType_(self, ctx: SQLParser.Type_Context):
        text = ctx.getText()
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
        return [each.accept(self) for each in ctx.where_clause()]

    def visitWhere_operator_expression(self, ctx: SQLParser.Where_operator_expressionContext):
        col = ctx.column.accept(self)
        pass

    def visitWhere_operator_select(self, ctx: SQLParser.Where_operator_selectContext):
        col = ctx.column.accept(self)
        pass

    def visitWhere_null(self, ctx: SQLParser.Where_nullContext):
        col = ctx.column.accept(self)
        pass

    def visitWhere_in_list(self, ctx: SQLParser.Where_in_listContext):
        col = ctx.column.accept(self)
        pass

    def visitWhere_in_select(self, ctx: SQLParser.Where_in_selectContext):
        col = ctx.column.accept(self)
        pass

    def visitWhere_like_string(self, ctx: SQLParser.Where_like_stringContext):
        col = ctx.column.accept(self)
        pass

    def visitColumn(self, ctx: SQLParser.ColumnContext) -> Col:
        table_name = ctx.Identifier(0)
        if(table_name is not None):
            table_name = str(table_name)
        col_name = str(ctx.Identifier(1))
        return Col(table_name, col_name)

    def visitExpression(self, ctx: SQLParser.ExpressionContext):
        pass

    def visitSet_clause(self, ctx: SQLParser.Set_clauseContext):
        pass

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
        return Col(None, None, Aggregator.COUNT)
    
    def visitIdentifiers(self, ctx: SQLParser.IdentifiersContext):
        idents: List[str] = [str(each) for each in ctx.Identifier()]
        return idents
    
    def visitOperator_(self, ctx: SQLParser.Operator_Context):
        if (ctx.EqualOrAssign() is not None):
            return Operator.OP_EQ
        if (ctx.Less() is not None):
            return Operator.OP_LT
        if (ctx.LessEqual() is not None):
            return Operator.OP_LE
        if (ctx.Greater() is not None):
            return Operator.OP_GT
        if (ctx.GreaterEqual() is not None):
            return Operator.OP_GE
        if (ctx.NotEqual() is not None):
            return Operator.OP_NE
    
    def visitAggregator(self, ctx: SQLParser.AggregatorContext):
        if (ctx.Count() is not None):
            return Aggregator.COUNT
        if (ctx.Sum() is not None):
            return Aggregator.SUM
        if (ctx.Avg() is not None):
            return Aggregator.AVG
        if (ctx.Min() is not None):
            return Aggregator.MIN
        if (ctx.Max() is not None):
            return Aggregator.MAX
    