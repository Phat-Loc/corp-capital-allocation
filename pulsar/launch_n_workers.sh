#!/bin/bash
echo "Launching $1 workers to extract text"
nohup python process_governer.py $1 python -c_args="extract_text_sub.py" >/dev/null 2>&1 &
