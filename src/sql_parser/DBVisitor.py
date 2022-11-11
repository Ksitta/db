from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import sm_manager
from src.type.type import FloatType, IntType, TypeEnum, VarcharType

class DBVisitor(SQLVisitor):
    def visitCreate_db(self, ctx : SQLParser.Create_dbContext):
        sm_manager.create_db(str(ctx.Identifier()))
    
    def visitShow_dbs(self, ctx : SQLParser.Show_dbsContext):
        sm_manager.show_dbs()

    def visitUse_db(self, ctx : SQLParser.Use_dbContext):
        sm_manager.open_db(str(ctx.Identifier()))

    def visitDrop_db(self, ctx : SQLParser.Drop_dbContext):
        sm_manager.drop_db(str(ctx.Identifier()))
        
    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        attrs : list = self.visitField_list(ctx.field_list())
        sm_manager.create_table(str(ctx.Identifier()), attrs)
        # pass

    def visitDescribe_table(self, ctx: SQLParser.Describe_tableContext):
        sm_manager.describe_table(str(ctx.Identifier()))

    def visitDrop_table(self, ctx: SQLParser.Drop_tableContext):
        sm_manager.drop_table(str(ctx.Identifier()))

    def visitField_list(self, ctx: SQLParser.Field_listContext):
        print(ctx.getChildCount())
        attrs = list()
        for i in range((ctx.getChildCount() + 1) // 2):
            attrs.append(self.visitField(ctx.getChild(i * 2)))
        return attrs

    def visitField(self, ctx: SQLParser.FieldContext):
        ctx.accept(self)

    def visitNormal_field(self, ctx: SQLParser.Normal_fieldContext):
        self.visitType_(ctx.type_)
        
    def visitType_(self, ctx: SQLParser.Type_Context):
        text = ctx.getText()
        if(text == 'INT'):
            return IntType()
        if(text == 'FLOAT'):
            return FloatType()
        if(text == 'VARCHAR'):
            return VarcharType(int(ctx.getChild(2).getText()))

    def visitPrimary_key(self, ctx: SQLParser.Primary_key_fieldContext):
        pass

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        pass


    
