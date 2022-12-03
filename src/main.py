#!/usr/bin/python3

import traceback
from antlr4 import *
from sql_parser.SQLLexer import SQLLexer
from sql_parser.SQLParser import SQLParser
from sql_parser.DBVisitor import DBVisitor
import signal
from sm_manager.sm_manager import sm_manager
from printer.printer import Printer
import time

def parser_command(line):
    input_stream = InputStream(line)

    # lexing
    lexer = SQLLexer(input_stream)
    stream = CommonTokenStream(lexer)

    # parsing
    parser = SQLParser(stream)
    tree = parser.program()
    if(parser.getNumberOfSyntaxErrors() > 0):
        return
    # use customized visitor to traverse AST
    visitor = DBVisitor()

    try:
        start_time = time.time()
        res = visitor.visit(tree)
        end_time = time.time()
        printer = Printer(res, end_time - start_time)
        printer.display()
    except Exception as e:
        print(repr(e))
        print("==============")
        print(traceback.format_exc())

exiting = False

def sigint_exit(signum, frame):
    global exiting
    if exiting:
        print("You choose to force exit. Data might be lost.")
        exit(0)
    print("\nSaving databases, press Ctrl+C again to force exit.")
    exiting = True
    sm_manager.close_db()
    print("Bye!")
    exit(0)
    
def main():
    signal.signal(signal.SIGINT, sigint_exit)
    res = ""
    while (True):
        using_db = sm_manager.get_using_db()
        if(using_db != ""):
            print("({}) ".format(using_db), end="")
        print(">>> ", end='')
        line = res
        while(True):
            line += input()
            if(';' in line):
                after_split = line.split(';', maxsplit=1)
                line = after_split[0] + ';'
                res = after_split[1]
                break
        if(line.lower() == "exit;"):
            sigint_exit(0, 0)
        parser_command(line)

if __name__ == '__main__':
    main()