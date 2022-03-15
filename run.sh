#!bin/bash
env=$(pwd)"/myenv"
if [ ! -d $env ]
then
	python3 -m venv $env
fi
source $env/bin/activate
pip3 install -r requirements.txt

IFS= read -r -p "Please insert your excel files:   " file_path

IFS= read -r -p "Please insert the tab name:       " tab_name

IFS= read -r -p "Please name your json_schema:     " json_schema_name
file_path_name=$(pwd)/$file_path
echo $file_path_name

./json_schema_converter.py "$file_path_name" "$tab_name" "$json_schema_name"
