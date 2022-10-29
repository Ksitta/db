#!/usr/bin/python3

import sys
from antlr4 import *
from sm_manager.sm_manager import SM_Manager
from sql_parser.SQLLexer import SQLLexer
from sql_parser.SQLParser import SQLParser
from sql_parser.DBVisitor import DBVisitor

def parser_command(line):
    input_stream = InputStream(line)

    # lexing
    lexer = SQLLexer(input_stream)
    stream = CommonTokenStream(lexer)

    # parsing
    parser = SQLParser(stream)
    tree = parser.program()

    # use customized visitor to traverse AST
    visitor = DBVisitor()
    
    try:
        visitor.visit(tree)
    except Exception as e:
        print(repr(e))

if __name__ == '__main__':
    SM_Manager()
    while True:
        print(">>> ", end='')
        line = input()
        parser_command(line)
