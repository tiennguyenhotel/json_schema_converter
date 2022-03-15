#!bin/bash
env=$(pwd)"/env"
if [ ! -d $env ]
then
	python3 -m venv $env
fi
source env/bin/activate
pip3 install -r requirements.txt

read -r -p "Please insert your excel files:   " file_path

read -r -p "Please insert the tab name:       " tab_name

read -r -p "Please name your json_schema:     " json_schema_name

./json_schema_converter.py $file_path $tab_name $json_schema_name
