# Generated from SQL.g4 by ANTLR 4.7.2
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .SQLParser import SQLParser
else:
    from SQLParser import SQLParser

# This class defines a complete listener for a parse tree produced by SQLParser.
class SQLListener(ParseTreeListener):

    # Enter a parse tree produced by SQLParser#program.
    def enterProgram(self, ctx:SQLParser.ProgramContext):
        pass

    # Exit a parse tree produced by SQLParser#program.
    def exitProgram(self, ctx:SQLParser.ProgramContext):
        pass


    # Enter a parse tree produced by SQLParser#statement.
    def enterStatement(self, ctx:SQLParser.StatementContext):
        pass

    # Exit a parse tree produced by SQLParser#statement.
    def exitStatement(self, ctx:SQLParser.StatementContext):
        pass


    # Enter a parse tree produced by SQLParser#create_db.
    def enterCreate_db(self, ctx:SQLParser.Create_dbContext):
        pass

    # Exit a parse tree produced by SQLParser#create_db.
    def exitCreate_db(self, ctx:SQLParser.Create_dbContext):
        pass


    # Enter a parse tree produced by SQLParser#drop_db.
    def enterDrop_db(self, ctx:SQLParser.Drop_dbContext):
        pass

    # Exit a parse tree produced by SQLParser#drop_db.
    def exitDrop_db(self, ctx:SQLParser.Drop_dbContext):
        pass


    # Enter a parse tree produced by SQLParser#show_dbs.
    def enterShow_dbs(self, ctx:SQLParser.Show_dbsContext):
        pass

    # Exit a parse tree produced by SQLParser#show_dbs.
    def exitShow_dbs(self, ctx:SQLParser.Show_dbsContext):
        pass


    # Enter a parse tree produced by SQLParser#use_db.
    def enterUse_db(self, ctx:SQLParser.Use_dbContext):
        pass

    # Exit a parse tree produced by SQLParser#use_db.
    def exitUse_db(self, ctx:SQLParser.Use_dbContext):
        pass


    # Enter a parse tree produced by SQLParser#show_tables.
    def enterShow_tables(self, ctx:SQLParser.Show_tablesContext):
        pass

    # Exit a parse tree produced by SQLParser#show_tables.
    def exitShow_tables(self, ctx:SQLParser.Show_tablesContext):
        pass


    # Enter a parse tree produced by SQLParser#show_indexes.
    def enterShow_indexes(self, ctx:SQLParser.Show_indexesContext):
        pass

    # Exit a parse tree produced by SQLParser#show_indexes.
    def exitShow_indexes(self, ctx:SQLParser.Show_indexesContext):
        pass


    # Enter a parse tree produced by SQLParser#load_data.
    def enterLoad_data(self, ctx:SQLParser.Load_dataContext):
        pass

    # Exit a parse tree produced by SQLParser#load_data.
    def exitLoad_data(self, ctx:SQLParser.Load_dataContext):
        pass


    # Enter a parse tree produced by SQLParser#dump_data.
    def enterDump_data(self, ctx:SQLParser.Dump_dataContext):
        pass

    # Exit a parse tree produced by SQLParser#dump_data.
    def exitDump_data(self, ctx:SQLParser.Dump_dataContext):
        pass


    # Enter a parse tree produced by SQLParser#create_table.
    def enterCreate_table(self, ctx:SQLParser.Create_tableContext):
        pass

    # Exit a parse tree produced by SQLParser#create_table.
    def exitCreate_table(self, ctx:SQLParser.Create_tableContext):
        pass


    # Enter a parse tree produced by SQLParser#drop_table.
    def enterDrop_table(self, ctx:SQLParser.Drop_tableContext):
        pass

    # Exit a parse tree produced by SQLParser#drop_table.
    def exitDrop_table(self, ctx:SQLParser.Drop_tableContext):
        pass


    # Enter a parse tree produced by SQLParser#describe_table.
    def enterDescribe_table(self, ctx:SQLParser.Describe_tableContext):
        pass

    # Exit a parse tree produced by SQLParser#describe_table.
    def exitDescribe_table(self, ctx:SQLParser.Describe_tableContext):
        pass


    # Enter a parse tree produced by SQLParser#insert_into_table.
    def enterInsert_into_table(self, ctx:SQLParser.Insert_into_tableContext):
        pass

    # Exit a parse tree produced by SQLParser#insert_into_table.
    def exitInsert_into_table(self, ctx:SQLParser.Insert_into_tableContext):
        pass


    # Enter a parse tree produced by SQLParser#delete_from_table.
    def enterDelete_from_table(self, ctx:SQLParser.Delete_from_tableContext):
        pass

    # Exit a parse tree produced by SQLParser#delete_from_table.
    def exitDelete_from_table(self, ctx:SQLParser.Delete_from_tableContext):
        pass


    # Enter a parse tree produced by SQLParser#update_table.
    def enterUpdate_table(self, ctx:SQLParser.Update_tableContext):
        pass

    # Exit a parse tree produced by SQLParser#update_table.
    def exitUpdate_table(self, ctx:SQLParser.Update_tableContext):
        pass


    # Enter a parse tree produced by SQLParser#select_table_.
    def enterSelect_table_(self, ctx:SQLParser.Select_table_Context):
        pass

    # Exit a parse tree produced by SQLParser#select_table_.
    def exitSelect_table_(self, ctx:SQLParser.Select_table_Context):
        pass


    # Enter a parse tree produced by SQLParser#select_table.
    def enterSelect_table(self, ctx:SQLParser.Select_tableContext):
        pass

    # Exit a parse tree produced by SQLParser#select_table.
    def exitSelect_table(self, ctx:SQLParser.Select_tableContext):
        pass


    # Enter a parse tree produced by SQLParser#alter_add_index.
    def enterAlter_add_index(self, ctx:SQLParser.Alter_add_indexContext):
        pass

    # Exit a parse tree produced by SQLParser#alter_add_index.
    def exitAlter_add_index(self, ctx:SQLParser.Alter_add_indexContext):
        pass


    # Enter a parse tree produced by SQLParser#alter_drop_index.
    def enterAlter_drop_index(self, ctx:SQLParser.Alter_drop_indexContext):
        pass

    # Exit a parse tree produced by SQLParser#alter_drop_index.
    def exitAlter_drop_index(self, ctx:SQLParser.Alter_drop_indexContext):
        pass


    # Enter a parse tree produced by SQLParser#alter_table_drop_pk.
    def enterAlter_table_drop_pk(self, ctx:SQLParser.Alter_table_drop_pkContext):
        pass

    # Exit a parse tree produced by SQLParser#alter_table_drop_pk.
    def exitAlter_table_drop_pk(self, ctx:SQLParser.Alter_table_drop_pkContext):
        pass


    # Enter a parse tree produced by SQLParser#alter_table_drop_foreign_key.
    def enterAlter_table_drop_foreign_key(self, ctx:SQLParser.Alter_table_drop_foreign_keyContext):
        pass

    # Exit a parse tree produced by SQLParser#alter_table_drop_foreign_key.
    def exitAlter_table_drop_foreign_key(self, ctx:SQLParser.Alter_table_drop_foreign_keyContext):
        pass


    # Enter a parse tree produced by SQLParser#alter_table_add_pk.
    def enterAlter_table_add_pk(self, ctx:SQLParser.Alter_table_add_pkContext):
        pass

    # Exit a parse tree produced by SQLParser#alter_table_add_pk.
    def exitAlter_table_add_pk(self, ctx:SQLParser.Alter_table_add_pkContext):
        pass


    # Enter a parse tree produced by SQLParser#alter_table_add_foreign_key.
    def enterAlter_table_add_foreign_key(self, ctx:SQLParser.Alter_table_add_foreign_keyContext):
        pass

    # Exit a parse tree produced by SQLParser#alter_table_add_foreign_key.
    def exitAlter_table_add_foreign_key(self, ctx:SQLParser.Alter_table_add_foreign_keyContext):
        pass


    # Enter a parse tree produced by SQLParser#alter_table_add_unique.
    def enterAlter_table_add_unique(self, ctx:SQLParser.Alter_table_add_uniqueContext):
        pass

    # Exit a parse tree produced by SQLParser#alter_table_add_unique.
    def exitAlter_table_add_unique(self, ctx:SQLParser.Alter_table_add_uniqueContext):
        pass


    # Enter a parse tree produced by SQLParser#field_list.
    def enterField_list(self, ctx:SQLParser.Field_listContext):
        pass

    # Exit a parse tree produced by SQLParser#field_list.
    def exitField_list(self, ctx:SQLParser.Field_listContext):
        pass


    # Enter a parse tree produced by SQLParser#normal_field.
    def enterNormal_field(self, ctx:SQLParser.Normal_fieldContext):
        pass

    # Exit a parse tree produced by SQLParser#normal_field.
    def exitNormal_field(self, ctx:SQLParser.Normal_fieldContext):
        pass


    # Enter a parse tree produced by SQLParser#primary_key_field.
    def enterPrimary_key_field(self, ctx:SQLParser.Primary_key_fieldContext):
        pass

    # Exit a parse tree produced by SQLParser#primary_key_field.
    def exitPrimary_key_field(self, ctx:SQLParser.Primary_key_fieldContext):
        pass


    # Enter a parse tree produced by SQLParser#foreign_key_field.
    def enterForeign_key_field(self, ctx:SQLParser.Foreign_key_fieldContext):
        pass

    # Exit a parse tree produced by SQLParser#foreign_key_field.
    def exitForeign_key_field(self, ctx:SQLParser.Foreign_key_fieldContext):
        pass


    # Enter a parse tree produced by SQLParser#type_.
    def enterType_(self, ctx:SQLParser.Type_Context):
        pass

    # Exit a parse tree produced by SQLParser#type_.
    def exitType_(self, ctx:SQLParser.Type_Context):
        pass


    # Enter a parse tree produced by SQLParser#value_lists.
    def enterValue_lists(self, ctx:SQLParser.Value_listsContext):
        pass

    # Exit a parse tree produced by SQLParser#value_lists.
    def exitValue_lists(self, ctx:SQLParser.Value_listsContext):
        pass


    # Enter a parse tree produced by SQLParser#value_list.
    def enterValue_list(self, ctx:SQLParser.Value_listContext):
        pass

    # Exit a parse tree produced by SQLParser#value_list.
    def exitValue_list(self, ctx:SQLParser.Value_listContext):
        pass


    # Enter a parse tree produced by SQLParser#value.
    def enterValue(self, ctx:SQLParser.ValueContext):
        pass

    # Exit a parse tree produced by SQLParser#value.
    def exitValue(self, ctx:SQLParser.ValueContext):
        pass


    # Enter a parse tree produced by SQLParser#where_and_clause.
    def enterWhere_and_clause(self, ctx:SQLParser.Where_and_clauseContext):
        pass

    # Exit a parse tree produced by SQLParser#where_and_clause.
    def exitWhere_and_clause(self, ctx:SQLParser.Where_and_clauseContext):
        pass


    # Enter a parse tree produced by SQLParser#where_operator_expression.
    def enterWhere_operator_expression(self, ctx:SQLParser.Where_operator_expressionContext):
        pass

    # Exit a parse tree produced by SQLParser#where_operator_expression.
    def exitWhere_operator_expression(self, ctx:SQLParser.Where_operator_expressionContext):
        pass


    # Enter a parse tree produced by SQLParser#where_operator_select.
    def enterWhere_operator_select(self, ctx:SQLParser.Where_operator_selectContext):
        pass

    # Exit a parse tree produced by SQLParser#where_operator_select.
    def exitWhere_operator_select(self, ctx:SQLParser.Where_operator_selectContext):
        pass


    # Enter a parse tree produced by SQLParser#where_null.
    def enterWhere_null(self, ctx:SQLParser.Where_nullContext):
        pass

    # Exit a parse tree produced by SQLParser#where_null.
    def exitWhere_null(self, ctx:SQLParser.Where_nullContext):
        pass


    # Enter a parse tree produced by SQLParser#where_in_list.
    def enterWhere_in_list(self, ctx:SQLParser.Where_in_listContext):
        pass

    # Exit a parse tree produced by SQLParser#where_in_list.
    def exitWhere_in_list(self, ctx:SQLParser.Where_in_listContext):
        pass


    # Enter a parse tree produced by SQLParser#where_in_select.
    def enterWhere_in_select(self, ctx:SQLParser.Where_in_selectContext):
        pass

    # Exit a parse tree produced by SQLParser#where_in_select.
    def exitWhere_in_select(self, ctx:SQLParser.Where_in_selectContext):
        pass


    # Enter a parse tree produced by SQLParser#where_like_string.
    def enterWhere_like_string(self, ctx:SQLParser.Where_like_stringContext):
        pass

    # Exit a parse tree produced by SQLParser#where_like_string.
    def exitWhere_like_string(self, ctx:SQLParser.Where_like_stringContext):
        pass


    # Enter a parse tree produced by SQLParser#column.
    def enterColumn(self, ctx:SQLParser.ColumnContext):
        pass

    # Exit a parse tree produced by SQLParser#column.
    def exitColumn(self, ctx:SQLParser.ColumnContext):
        pass


    # Enter a parse tree produced by SQLParser#expression.
    def enterExpression(self, ctx:SQLParser.ExpressionContext):
        pass

    # Exit a parse tree produced by SQLParser#expression.
    def exitExpression(self, ctx:SQLParser.ExpressionContext):
        pass


    # Enter a parse tree produced by SQLParser#set_clause.
    def enterSet_clause(self, ctx:SQLParser.Set_clauseContext):
        pass

    # Exit a parse tree produced by SQLParser#set_clause.
    def exitSet_clause(self, ctx:SQLParser.Set_clauseContext):
        pass


    # Enter a parse tree produced by SQLParser#selectors.
    def enterSelectors(self, ctx:SQLParser.SelectorsContext):
        pass

    # Exit a parse tree produced by SQLParser#selectors.
    def exitSelectors(self, ctx:SQLParser.SelectorsContext):
        pass


    # Enter a parse tree produced by SQLParser#selector.
    def enterSelector(self, ctx:SQLParser.SelectorContext):
        pass

    # Exit a parse tree produced by SQLParser#selector.
    def exitSelector(self, ctx:SQLParser.SelectorContext):
        pass


    # Enter a parse tree produced by SQLParser#identifiers.
    def enterIdentifiers(self, ctx:SQLParser.IdentifiersContext):
        pass

    # Exit a parse tree produced by SQLParser#identifiers.
    def exitIdentifiers(self, ctx:SQLParser.IdentifiersContext):
        pass


    # Enter a parse tree produced by SQLParser#operator_.
    def enterOperator_(self, ctx:SQLParser.Operator_Context):
        pass

    # Exit a parse tree produced by SQLParser#operator_.
    def exitOperator_(self, ctx:SQLParser.Operator_Context):
        pass


    # Enter a parse tree produced by SQLParser#aggregator.
    def enterAggregator(self, ctx:SQLParser.AggregatorContext):
        pass

    # Exit a parse tree produced by SQLParser#aggregator.
    def exitAggregator(self, ctx:SQLParser.AggregatorContext):
        pass


