from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import SM_Manager

class DBVisitor(SQLVisitor):
    def visitCreate_db(self, ctx : SQLParser.Create_dbContext):
        SM_Manager().create_db(str(ctx.Identifier()))
    
    def visitShow_dbs(self, ctx : SQLParser.Show_dbsContext):
        SM_Manager().show_dbs()

    def visitUse_db(self, ctx : SQLParser.Use_dbContext):
        SM_Manager().open_db(str(ctx.Identifier()))

    def visitDrop_db():
        pass
        
    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        attrs : list = self.visitField_list(ctx.field_list())
        SM_Manager().create_table(str(ctx.Identifier()), attrs)
        # pass

    def visitDescribe_table(self, ctx: SQLParser.Describe_tableContext):
        SM_Manager().describe_table(str(ctx.Identifier()))

    def visitField_list(self, ctx: SQLParser.Field_listContext):
        print(ctx.getChildCount())
        attrs = list()
        for i in range((ctx.getChildCount() + 1) // 2):
            attrs.append(self.visitField(ctx.getChild(i * 2)))
        return attrs

    def visitField(self, ctx: SQLParser.FieldContext):
        ctx.accept(self)

    def visitNormal_field(self, ctx: SQLParser.Normal_fieldContext):
        print(ctx.Identifier())

    def visitPrimary_key(self, ctx: SQLParser.Primary_key_fieldContext):
        pass

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        pass


    
