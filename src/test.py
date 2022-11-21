import os
import shutil
import traceback
import config as cf
import test.test_paged_file as test_pf
import test.test_record_management as test_rm

if __name__ == '__main__':
    if os.path.exists(cf.TEST_ROOT):
        raise Exception(f'Test root exist.')
    os.mkdir(cf.TEST_ROOT)
    try: # add your unit test function here
        test_pf.test()
        test_rm.test()
    except Exception as e:
        traceback.print_exc()
    shutil.rmtree(cf.TEST_ROOT)
