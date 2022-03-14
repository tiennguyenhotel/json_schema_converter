 # import
import pandas
import json
import math
import os

import pandas.io.sql as sqlio

import sys

class schema_converter:
    def __init__(self, pandas_data_frame):
        self.pandas_data_frame = pandas_data_frame
        self.table_list = self.get_table_list()

    def get_table_list(self):
        table_name = self.pandas_data_frame["Table_Name"]
        table_list = self.remove_dup(table_name)
        return table_list

    def get_graphql_format_to_local_file(self):
        # desktop path

        desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        folder = "schema_results"
        full = os.path.join(desktop, folder)
        # check if the folder already exists
        exist = os.path.exists(full)
        if exist == False:
            # if there is no existed file
            # we are going to create that directory
            os.makedirs(full)
        # query using table_list name
        for i in self.table_list:
            # sort table using table_list
            frame = self.pandas_data_frame.query('Table_Name=="{}"'.format(i))
            # create a new text file with table name Name
            with open(os.path.join(full,'{}_graphql.txt'.format(i)), 'w') as text_file:
                text_file.write("type {}".format(i) + "{")
            name = [i for i in frame["Name"]]
            type = [i for i in frame["Type"]]
            required = [i for i in frame["Is it required?"]]
            list = [i for i in frame["is it a list?"]]

            length = len(name)
            for k in range(length):
                type_s = type[k]
                required_s = required[k]
                list_s = list[k]
                name_s = name[k] + ":"
                if list_s == "y" or list_s == "Y":
                    type_s = "[" + type_s + "]"
                if required_s == "y" or required_s == "Y":
                    type_s = type_s + "!"
                final = str(name_s) + str(type_s)
                with open(os.path.join(full,'{}_graphql.txt'.format(i)), 'a') as f:
                    f.write("\n")
                    f.write(final)
            with open(os.path.join(full,'{}_graphql.txt'.format(i)), 'a') as f:
                f.write("\n")
                f.write("}")
    def get_json_schema_variable(self):

        array_return=[]
        for i in self.table_list:
            json_var = {}
            property_list={}

            # sort table using table_list
            frame = self.pandas_data_frame.query('Table_Name=="{}"'.format(i))
            # getting description
            table_description = frame["Table_Description"]
            table_description = self.remove_dup(table_description)[0]
            json_schema_id = self.remove_dup(frame["json_schema_id"])[0]
            json_var["$schema"]="https://json-schema.org/draft/2020-12/schema"
            json_var["$id"]=json_schema_id
            json_var["title"]=i
            json_var["description"]=table_description
            json_var["type"]="object"
            #create new json variable to store property
            property_json={}
            name = frame["Name"]
            type = self.parse_correct_type_json(frame["Type"])

            required = frame["Is it required?"]
            list = frame["is it a list?"]
            description = frame["Description"]
            min = frame["min"]
            max = frame["max"]
            regex=frame["regex_pattern"]
            ## enum value restriction.
            enum_val=frame["enum_value"]

            zipped = zip(name, type, required, list, description, min, max,enum_val,regex)

            required_name_list = []


            for index, (n, t, r, l, d, mi, ma,enum_val,regex_val) in enumerate(zipped):
                #make type into string if possibble

                if r == "y" or r == "Y":
                    required_name_list.append(n)
                inner_property_list={}
                # parsing description
                try:
                    inner_property_list["description"]=str(d.replace("\t","").replace("\u2019","'").replace("\u00a0"," ").replace("\n"," ").replace("\u201c","").replace("\u201d","").replace("\u2022","").replace("\u2013","-"))
                except:
                    pass
                # check if type is an array that has length more than 1 after split
                t_clean=t.replace(" ","").split(",")

                length=len(t_clean)
                # if there is more than 1 element in the type list

                if length>1:

                    t=t_clean
                    # the first element should be any of
                    array_append=[]


                    #check if it is a string or int
                    for t1 in t:
                        # if it is a string
                        inner_property_list_anyof = {}

                        if t1=="string":
                            # we are going to check if there is a minimum value
                            if math.isnan(mi) == False:
                                inner_property_list_anyof["type"] = "string"
                                inner_property_list_anyof["minLength"] = int(mi)
                                # we are going to check if there is a maximum value

                                if math.isnan(ma) == False:
                                    inner_property_list_anyof["maxLength"] = int(ma)
                            else:
                                inner_property_list_anyof["type"]="string"
                                if math.isnan(ma) == False:
                                    inner_property_list_anyof["maxLength"] = int(ma)
                            if pandas.isna(regex_val) == True:
                                pass
                            else:
                                inner_property_list_anyof["pattern"] = regex_val
                        elif t1 == "date":
                            inner_property_list_anyof["type"] = "string"
                            inner_property_list_anyof["format"] = "date"
                        elif t1 == "email":
                            inner_property_list_anyof["type"] = "string"
                            inner_property_list_anyof["format"] = "email"
                        elif t1 == "date-time":
                            inner_property_list_anyof["type"] = "string"
                            inner_property_list_anyof["format"] = "date-time"
                        elif t1=="integer":
                            if math.isnan(mi)==False:
                                inner_property_list_anyof["type"]=t1.lower()
                                inner_property_list_anyof["minimum"]=int(mi)
                                if math.isnan(ma)==False:
                                    inner_property_list_anyof["maximum"]=int(ma)
                            else:
                                inner_property_list_anyof["type"]=t1.lower()
                                if math.isnan(ma)==False:
                                    inner_property_list_anyof["maximum"]=int(ma)
                        elif t1=="enum":
                            val=self.enum_transformation(enum_val)
                            inner_property_list_anyof["enum"] =val
                        elif t1=="number":

                            if math.isnan(mi)==False:
                                inner_property_list_anyof["type"]=t1.lower()
                                inner_property_list_anyof["minimum"]=int(mi)
                                if math.isnan(ma)==False:
                                    inner_property_list_anyof["maximum"]=int(ma)
                            else:
                                inner_property_list_anyof["type"]=t1.lower()
                                if math.isnan(ma)==False:
                                    inner_property_list_anyof["maximum"]=int(ma)
                        object_length=len(inner_property_list_anyof)
                        if object_length==0:
                            continue
                        else:
                            array_append.append(inner_property_list_anyof)

                    inner_property_list["anyOf"]=array_append
                    # addd these element to inner_property_list




                else:
                    if t=="date":
                        inner_property_list["type"]="string"
                        inner_property_list["format"]="date"
                    elif t=="email":
                        inner_property_list["type"]="string"
                        inner_property_list["format"]="email"
                    elif t=="date-time":
                        inner_property_list["type"]="string"
                        inner_property_list["format"]="date-time"
                    elif t=="string":
                        if math.isnan(mi) == False:
                            inner_property_list["type"]="string"
                            inner_property_list["minLength"]=int(mi)
                            if math.isnan(ma)==False:
                                inner_property_list["maxLength"]=int(ma)
                        else:
                            inner_property_list["type"]="string"
                            if math.isnan(ma) == False:
                                inner_property_list["maxLength"] = int(ma)
                        if pandas.isna(regex_val)==True:
                            pass
                        else:
                            inner_property_list["pattern"]=regex_val
                    elif t=="integer":
                        if math.isnan(mi)==False:
                            inner_property_list["type"]=t.lower()
                            inner_property_list["minimum"]=int(mi)
                            if math.isnan(ma)==False:
                                inner_property_list["maximum"]=int(ma)
                        else:
                            inner_property_list["type"]=t.lower()
                            if math.isnan(ma)==False:
                                inner_property_list["maximum"]=int(ma)
                    elif t=="enum":
                        #transform enum array
                        val=self.enum_transformation(enum_val)

                        inner_property_list["enum"]=val
                    elif t== "number":

                        if math.isnan(mi) == False:

                            inner_property_list["type"] = t.lower()
                            inner_property_list["minimum"] = int(mi)
                            if math.isnan(ma) == False:
                                inner_property_list["maximum"] = int(ma)
                        else:

                            inner_property_list["type"] = t.lower()

                            if math.isnan(ma) == False:
                                inner_property_list["maximum"] = int(ma)
                property_list[n]=inner_property_list
            json_var["properties"]=property_list
            #get required variable
            json_var["required"]=required_name_list
            json_var=json.dumps(json_var,indent=4)

            array_return.append(json_var)
        return array_return

    def enum_transformation(self,data):
        # first we need to serialize the data and convert that into an array
        array=str(data).replace(" ","").split(",")

        length=len(array)
        array_return = []

        if length==1 and array[0]=="nan":
            array_return=['NA']
            return array_return



        for i in array:

            value=str(i)
            array_return.append(value)

        return array_return


    def parse_correct_type_json(self, array):

        new_array = [str(i).replace("String", "string").replace("Int", "integer").replace("int","integer").replace("Date","date").replace("Email","email").replace("Number","number") for i in array]

        return new_array

    def remove_dup(self, data):
        no_dup = []
        for i in data:
            if i not in no_dup:
                no_dup.append(i)
        return no_dup
    def variable_to_json_file(self,json_variable,name="json_schema"):
        desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        json_file=open(os.path.join(desktop,f"{name}.json"),"w")
        json_file.write(json_variable)
        json_file.close()




file=pandas.read_excel(sys.argv[1],sys.argv[2])
convert_engine=schema_converter(file)
a=convert_engine.get_json_schema_variable()[0]
convert_engine.variable_to_json_file(a,name=sys.argv[3])
