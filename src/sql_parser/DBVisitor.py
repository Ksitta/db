from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import sm_manager
from type.type import TypeEnum
from table.table import Column

class DBVisitor(SQLVisitor):
    def visitCreate_db(self, ctx: SQLParser.Create_dbContext):
        return sm_manager.create_db(str(ctx.Identifier()))

    def visitShow_dbs(self, ctx: SQLParser.Show_dbsContext):
        res = sm_manager.show_dbs()
        print(res)

    def visitUse_db(self, ctx: SQLParser.Use_dbContext):
        return sm_manager.open_db(str(ctx.Identifier()))

    def visitDrop_db(self, ctx: SQLParser.Drop_dbContext):
        return sm_manager.drop_db(str(ctx.Identifier()))

    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        self._attrs: list = list()
        ctx.field_list().accept(self)
        return sm_manager.create_table(str(ctx.Identifier()), self._attrs)

    def visitDescribe_table(self, ctx: SQLParser.Describe_tableContext):
        return sm_manager.describe_table(str(ctx.Identifier()))

    def visitDrop_table(self, ctx: SQLParser.Drop_tableContext):
        return sm_manager.drop_table(str(ctx.Identifier()))

    def visitField_list(self, ctx: SQLParser.Field_listContext):
        for each in ctx.children:
            each.accept(self)

    def visitField(self, ctx: SQLParser.FieldContext):
        ctx.accept(self)

    def visitNormal_field(self, ctx: SQLParser.Normal_fieldContext):
        ctx.type_().accept(self)
        ident = str(ctx.Identifier())
        # self._attrs.append(Column(ident, self._type, self._type_size))

    def visitType_(self, ctx: SQLParser.Type_Context):
        text = ctx.getText()
        if (text == 'INT'):
            self._type = TypeEnum.INT
            self._type_size = 4
        if (text == 'FLOAT'):
            self._type = TypeEnum.FLOAT
            self._type_size = 8
        if (text == 'VARCHAR'):
            self._type = TypeEnum.VARCHAR
            self._type_size = int(ctx.Integer().getText())
            
    def visitPrimary_key_field(self, ctx: SQLParser.Primary_key_fieldContext):
        self._idents = ctx.identifiers().accept(self)

    def visitIdentifiers(self, ctx: SQLParser.IdentifiersContext):
        idents: list = list()
        for each in ctx.children:
            if each.getText() != ',':
                idents.append(str(each.getText()))
        return idents

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        self._constraint_name = str(ctx.Identifier(0))
        self._target_table = str(ctx.Identifier(1))
        self._local_idents = ctx.identifiers(0).accept(self)
        self._target_idents = ctx.identifiers(1).accept(self)

    def visitAlter_add_index(self, ctx: SQLParser.Alter_add_indexContext):
        pass
    
    def visitAlter_drop_index(self, ctx: SQLParser.Alter_drop_indexContext):
        pass
    
    def visitAlter_table_drop_pk(self, ctx: SQLParser.Alter_table_drop_pkContext):
        pass
    
    def visitAlter_table_drop_foreign_key(self, ctx: SQLParser.Alter_table_drop_foreign_keyContext):
        pass
    
    def visitAlter_table_add_pk(self, ctx: SQLParser.Alter_table_add_pkContext):
        pass
    
    def visitAlter_table_add_foreign_key(self, ctx: SQLParser.Alter_table_add_foreign_keyContext):
        pass
    
    def visitAlter_table_add_unique(self, ctx: SQLParser.Alter_table_add_uniqueContext):
        pass
    
    