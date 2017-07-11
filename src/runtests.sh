#!/bin/bash
python base/test_prototype.py $*
PYTHONPATH=.:$PYTHONPATH python schedulers/tests.py $*
