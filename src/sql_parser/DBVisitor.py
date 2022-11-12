from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import sm_manager
from type.type import FloatType, IntType, VarcharType


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
        self._attrs.append((ident, self._type))

    def visitType_(self, ctx: SQLParser.Type_Context):
        text = ctx.getText()
        if (text == 'INT'):
            self._type = IntType()
        if (text == 'FLOAT'):
            self._type = FloatType()
        if (text == 'VARCHAR'):
            self._type = VarcharType(int(ctx.getChild(2).getText()))

    def visitPrimary_key(self, ctx: SQLParser.Primary_key_fieldContext):
        pass

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        pass
