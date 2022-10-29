#!/usr/bin/python3

import sys
from antlr4 import *
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
        print(e.args)

if __name__ == '__main__':
    while True:
        print(">>> ", end='')
        line = input()
        parser_command(line)
