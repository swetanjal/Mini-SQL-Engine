if [ $# -ne 1 ]; then
    echo "Incorrect usage! Syntax: ./20171077.sh \"SQL QUERY\""
    exit 1
fi
python3 main.py "$1"