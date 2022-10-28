import config as cf
import test.test_file_manager as test_file_manager

if __name__ == '__main__':
    cf.BUFFER_CAPACITY = 4
    cf.PAGE_SIZE = 16
    test_file_manager.test()
