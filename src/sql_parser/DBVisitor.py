from sql_parser.SQLVisitor import SQLVisitor
from sql_parser.SQLParser import SQLParser
from sm_manager.sm_manager import SM_Manager

class DBVisitor(SQLVisitor):
    def visitCreate_db(self, ctx : SQLParser.Create_dbContext):
        pass
    
    def visitShow_dbs(self, ctx : SQLParser.Show_dbsContext):
        print(ctx)

    def visitUse_db(self, ctx : SQLParser.Use_dbContext):
        SM_Manager.open_db(ctx.Identifier())

    def visitDrop_db():
        pass
        
    pass
