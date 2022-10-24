#!/usr/bin/python3

from antlr4 import *
from parser.SQLLexer import SQLLexer
from parser.SQLParser import SQLParser
from parser.SQLVisitor import SQLVisitor


class DbVisitor(SQLVisitor):
    pass


def parser_command(line) -> float:
    input_stream = InputStream(line)

    # lexing
    lexer = SQLLexer(input_stream)
    stream = CommonTokenStream(lexer)

    # parsing
    parser = SQLParser(stream)
    tree = parser.program()

    # use customized visitor to traverse AST
    visitor = DbVisitor()
    try:
        visitor.visit(tree)
    except:
        print("error argument")

if __name__ == '__main__':
    while True:
        print(">>> ", end='')
        line = input()
        parser_command(line)
