#!/usr/bin/python3

import traceback
from antlr4 import *
from sql_parser.SQLLexer import SQLLexer
from sql_parser.SQLParser import SQLParser
from sql_parser.DBVisitor import DBVisitor
import signal
from sm_manager.sm_manager import sm_manager

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
        res = visitor.visit(tree)
    except Exception as e:
        print(repr(e))
        print("==============")
        print(traceback.format_exc())

exiting = False

def sigint_exit(signum, frame):
    global exiting
    if exiting:
        print("\nYou choose to force exit. Data might be lost.")
        exit(0)
    print("\nSaving databases, press Ctrl+C again to force exit.")
    exiting = True
    sm_manager.close_db()
    print("Bye!")
    exit(0)
    

if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_exit)
    while True:
        print(">>> ", end='')
        line = input()
        parser_command(line)
