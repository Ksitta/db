from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import sm_manager
from table.table import Column
from typing import List
from config import *
class DBVisitor(SQLVisitor):
    def visitCreate_db(self, ctx: SQLParser.Create_dbContext):
        return sm_manager.create_db(str(ctx.Identifier()))

    def visitShow_dbs(self, ctx: SQLParser.Show_dbsContext):
        res = sm_manager.show_dbs()
        print(res)   # TODO: use an elegant way to implement this

    def visitUse_db(self, ctx: SQLParser.Use_dbContext):
        return sm_manager.open_db(str(ctx.Identifier()))

    def visitDrop_db(self, ctx: SQLParser.Drop_dbContext):
        return sm_manager.drop_db(str(ctx.Identifier()))

    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        self._attrs: list = list()
        self._pk: list = list()
        self._fk: dict = dict()
        ctx.field_list().accept(self)
        res = sm_manager.create_table(str(ctx.Identifier()), self._attrs, self._pk, self._fk)
        print(res)  # TODO: use an elegant way to implement this

    def visitDescribe_table(self, ctx: SQLParser.Describe_tableContext):
        sm_manager.describe_table(str(ctx.Identifier()))

    def visitDrop_table(self, ctx: SQLParser.Drop_tableContext):
        return sm_manager.drop_table(str(ctx.Identifier()))

    def visitField_list(self, ctx: SQLParser.Field_listContext):
        for each in ctx.field():
            each.accept(self)

    def visitField(self, ctx: SQLParser.FieldContext):
        ctx.accept(self)
       
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
    
    def visitInsert_into_table(self, ctx: SQLParser.Insert_into_tableContext):
        table_name = str(ctx.Identifier())
        values = ctx.value_lists().accept(self)
        sm_manager.insert(table_name, values)
        
    def visitShow_tables(self, ctx: SQLParser.Show_tablesContext):
        tables = sm_manager.show_tables()
        print(tables)

    def visitNormal_field(self, ctx: SQLParser.Normal_fieldContext):
        ctx.type_().accept(self)
        ident: str = str(ctx.Identifier())
        nullable: bool = ctx.Null() is not None
        default_val = ctx.value()
        if default_val is not None:
            if self._type == TYPE_INT:
                default_val: int = int(default_val)
            elif self._type == TYPE_FLOAT:
                default_val: float = float(default_val)
            elif self._type == TYPE_STR:
                default_val: str = str(default_val)
        self._attrs.append(Column(ident, self._type, self._type_size, nullable, default_val))

    def visitType_(self, ctx: SQLParser.Type_Context):
        text = ctx.getText()
        if (text == 'INT'):
            self._type = TYPE_INT
            self._type_size = 4
        if (text == 'FLOAT'):
            self._type = TYPE_FLOAT
            self._type_size = 8
        if (text == 'VARCHAR'):
            self._type = TYPE_STR
            self._type_size = int(ctx.Integer().getText())
            
    def visitPrimary_key_field(self, ctx: SQLParser.Primary_key_fieldContext):
        self._pk: List[str] = ctx.identifiers().accept(self)

    def visitIdentifiers(self, ctx: SQLParser.IdentifiersContext):
        idents: List[str] = [str(each) for each in ctx.Identifier()]
        return idents

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        self._fk: dict = {}
        self._fk["constraint_name"] = str(ctx.Identifier(0))
        self._fk["target_table"] = str(ctx.Identifier(1))
        self._fk["local_idents"] = ctx.identifiers(0).accept(self)
        self._fk["target_idents"] = ctx.identifiers(1).accept(self)
        if(len(self._fk["local_idents"]) != len(self._fk["target_idents"])):
            raise Exception("Foreign key error: local idents and target idents are not equal.")

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
    
    def visitSelectors(self, ctx: SQLParser.SelectorsContext):
        return super().visitSelectors(ctx)
    
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
    
    def visitWhere_and_clause(self, ctx: SQLParser.Where_and_clauseContext):
        return [each.accept(self) for each in ctx.where_clause()]
    
    def visitWhere_operator_expression(self, ctx: SQLParser.Where_operator_expressionContext):
        pass
    
    def visitWhere_null(self, ctx: SQLParser.Where_nullContext):
        pass
    
    def visitWhere_in_list(self, ctx: SQLParser.Where_in_listContext):
        pass
    
    def visitWhere_in_select(self, ctx: SQLParser.Where_in_selectContext):
        pass
    
    def visitWhere_like_string(self, ctx: SQLParser.Where_like_stringContext):
        pass
    
    def visitSelect_table(self, ctx: SQLParser.Select_tableContext):
        pass
    
    def visitSelectors(self, ctx: SQLParser.SelectorsContext):
        pass
    
    def visitSelector(self, ctx: SQLParser.SelectorContext):
        pass
    
    def visitOperator_(self, ctx: SQLParser.Operator_Context):
        pass
    
    def visitSet_clause(self, ctx: SQLParser.Set_clauseContext):
        pass
    
    