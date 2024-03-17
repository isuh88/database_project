from lark import Lark, Transformer
from berkeleydb import db
from json import dumps, loads
from lark.exceptions import UnexpectedCharacters


class MyTransformer(Transformer):                   # Transformer class
    def __init__(self, db_path):
        self.db_path = db_path
        self.result = None

    def create_table_query(self, items):

        table_name = items[2].children[0].value.lower()                             # Get table name
        column_definitions = items[3].find_data('column_definition')                # Get column definitions
        constraint_definitions = items[3].find_data('table_constraint_definition')  # Get constraint definitions

        # initialize table info.
        column_names = []
        column_types = []
        primary_keys = []
        foreign_key_values = []
        cols_is_nullable = []

        referencing_tables = []
        referencing_columns = []
        uuid = 0
        record_ids = []
        is_pk_assigned = False

        for column_def in column_definitions:                                      # for each parsed column
            col_name = column_def.children[0].children[0].value.lower()
            col_type = column_def.children[1].children[0].value.lower()

            # DuplicateColumnDefError
            if col_name in column_names:
                self.result = [True, f"Create table has failed: column definition is duplicated"]
                return

            if col_type == "char":
                char_length = int(column_def.children[1].children[2].value)
                # CharLengthError
                if char_length < 1:
                    self.result = [True, "Char length should be over 0"]
                    return
                
                col_type += "(" + str(column_def.children[1].children[2].value) + ")"       # Add char length
            if column_def.children[2] != None:                                      # column is not nullable when not null constraint is given
                col_is_nullable = False
            else:
                col_is_nullable = True

            column_names.append(col_name)
            column_types.append(col_type)
            cols_is_nullable.append(col_is_nullable)

        for constraint_def in constraint_definitions:                               # for each parsed constraint
            constraint_type = constraint_def.children[0]

            if constraint_type.children[0].value.lower() == "primary":                      # primary key constraint
                 # DuplicatePrimaryKeyDefError
                if is_pk_assigned:
                    self.result = [True, "Create table has failed: primary key definition is duplicated"]
                    return
                
                pk_cols = constraint_type.children[2].find_data('column_name')
                for pk_col in pk_cols:
                    col_name = pk_col.children[0].value.lower()
                    # NonExistingColumnDefError(#colName)
                    if col_name not in column_names:
                        self.result = [True, f"Create table has failed: '{col_name}' does not exist in column definition"]
                        return
                    primary_keys.append(col_name)
                    cols_is_nullable[column_names.index(col_name)] = False          # primary key is not nullable
                is_pk_assigned = True                                               # primary key is set

            else:                                                                   # foreign key constraint
                fk_col = constraint_type.children[2].children[1].children[0].value.lower()
                reference_table = constraint_type.children[4].children[0].value.lower()
                reference_col = constraint_type.children[5].children[1].children[0].value.lower()

                # NonExistingColumnDefError(#colName)
                if fk_col not in column_names:
                    self.result = [True, f"Create table has failed: '{fk_col}' does not exist in column definition"]
                    return
                
                # SelfReferenceError(CustomError)
                if reference_table == table_name:
                    self.result = [True, "Create table has failed: foreign key references itself"]
                    return

                foreign_key_values.append(fk_col)
                referencing_tables.append(reference_table)
                referencing_columns.append(reference_col)

        ########################################## things to put in database ##############################################
        # table_name                (table_list)    *append to table_list
        # column_names              (table_list/{table_name}/column_names)
        # primary_key               (table_list/{table_name}/primary_key)
        # referencing_tables        (table_list/{table_name}/referencing_tables)
        # referencing_columns       (table_list/{table_name}/referencing_columns)
        # foreign_key_values        (table_list/{table_name}/foreign_key_values)
        # col_type                  (table_list/{table_name}/{column_name}/type))    
        # col_is_nullable           (table_list/{table_name}/{column_name}/is_nullable)  
        # col_is_pk                 (table_list/{table_name}/{column_name}/is_pk)
        # col_is_fk                 (table_list/{table_name}/{colomn_name}/is_fk)
        # uuid                      (table_list/{table_name}/uuid)
        # record_ids                (table_list/{table_name}/record_ids)
        ########################################## things to put in database ##############################################

        database = db.DB()
        database.open(self.db_path, None, db.DB_HASH, db.DB_CREATE)             # Create database

        # TableExistenceError
        if b"table_list" in database.keys():
            if table_name in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
                self.result = [True, "Create table has failed: table with the same name already exists"]
                return
            
        for i in range(len(referencing_tables)):
            # ReferenceTableExistenceError
            if referencing_tables[i] not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
                self.result = [True, "Create table has failed: foreign key references non existing table"]
                return
            
            # ReferenceColumnExistenceError
            if referencing_columns[i] not in loads(database.get(bytes("table_list/" + referencing_tables[i] + "/column_names", "utf-8")).decode("utf-8")):    
                self.result = [True, "Create table has failed: foreign key references non existing column"]
                return
            
            # ReferenceTypeError
            if column_types[column_names.index(foreign_key_values[i])] != database.get(bytes("table_list/" + referencing_tables[i] + "/" + referencing_columns[i] + "/type", "utf-8")).decode("utf-8"):
                self.result = [True, "Create table has failed: foreign key references wrong type"]
                return
            
            # ReferenceNonPrimaryKeyError
            if [referencing_columns[i]] != loads(database.get(bytes("table_list/" + referencing_tables[i] + "/primary_key", "utf-8")).decode("utf-8")):
                self.result = [True, "Create table has failed: foreign key references non primary key column"]
                return

        if b"table_list" in database.keys():
            table_list = loads(database.get(bytes("table_list", "utf-8")).decode("utf-8"))
        else:
            table_list = []                                                         # Create table list if not exists
        table_list.append(table_name)
        # put table metadata in database
        database.put(bytes("table_list", "utf-8"), bytes(dumps(table_list), "utf-8"))
        database.put(bytes("table_list/" + table_name + "/column_names", "utf-8"), bytes(dumps(column_names), "utf-8"))
        database.put(bytes("table_list/" + table_name + "/primary_key", "utf-8"), bytes(dumps(primary_keys), "utf-8"))
        database.put(bytes("table_list/" + table_name + "/referencing_tables", "utf-8"), bytes(dumps(referencing_tables), "utf-8"))
        database.put(bytes("table_list/" + table_name + "/referencing_columns", "utf-8"), bytes(dumps(referencing_columns), "utf-8"))
        database.put(bytes("table_list/" + table_name + "/foreign_key_values", "utf-8"), bytes(dumps(foreign_key_values), "utf-8"))
        database.put(bytes("table_list/" + table_name + "/uuid", "utf-8"), bytes(str(uuid), "utf-8"))
        database.put(bytes("table_list/" + table_name + "/record_ids", "utf-8"), bytes(dumps(record_ids), "utf-8"))
        # put column metadata in database
        for i in range(len(column_names)):
            database.put(bytes("table_list/" + table_name + "/" + column_names[i] + "/type", "utf-8"), bytes(column_types[i], "utf-8"))
            database.put(bytes("table_list/" + table_name + "/" + column_names[i] + "/is_nullable", "utf-8"), bytes(str(cols_is_nullable[i]), "utf-8"))
            database.put(bytes("table_list/" + table_name + "/" + column_names[i] + "/is_pk", "utf-8"), bytes(str(column_names[i] in primary_keys), "utf-8"))
            database.put(bytes("table_list/" + table_name + "/" + column_names[i] + "/is_fk", "utf-8"), bytes(str(column_names[i] in foreign_key_values), "utf-8"))

        database.close()                                                        # Close database
        self.result = [True, f"'{table_name}' table is created"]
        return 

    def drop_table_query(self, items):

        table_name = items[2].children[0].value.lower()

        database = db.DB()
        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        except db.DBNoSuchFileError:                                        # NoSuchFile
            self.result = [True, "No such table"]
            return
        
        cursor = database.cursor()                                          # Create cursor
        x = cursor.first()                                                  # Get first key-value pair

        # NoSuchTable
        if b"table_list" not in database.keys() or table_name not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
            self.result = [True, "No such table"]
            return
        
        # DropReferencedTableError
        while x is not None:
            if x[0].endswith(b"/referencing_tables"):
                if table_name in loads(x[1].decode("utf-8")):
                    self.result = [True, f"Drop table has failed: '{table_name}' is referenced by other table"]
                    return
            x = cursor.next()

        # delete all data related to given table
        x = cursor.first()
        while x is not None:
            if x[0].startswith(bytes("table_list/" + table_name, "utf-8")):
                database.delete(x[0])
            x = cursor.next()

        # delete table from table list
        table_list = loads(database.get(bytes("table_list", "utf-8")).decode("utf-8"))
        table_list.remove(table_name)
        database.put(bytes("table_list", "utf-8"), bytes(dumps(table_list), "utf-8"))

        cursor.close()                                                        # Close cursor
        database.close()                                                      # Close database
        self.result = [True, f"'{table_name}' table is dropped"]
        return
    
    def desc_query(self, items):
        table_name = items[1].children[0].value.lower()

        database = db.DB()
        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        except db.DBNoSuchFileError:                                        # NoSuchFile
            self.result = [True, "No such table"]
            return

        # NoSuchTable
        if b"table_list" not in database.keys() or table_name not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
            self.result = [True, "No such table"]
            return
        
        column_names = loads(database.get(bytes("table_list/" + table_name + "/column_names", "utf-8")).decode("utf-8"))

        # print formatted table info.
        result = print_table_schema(table_name, column_names, database)

        database.close()                                                    # Close database
        self.result = [False, result]
        return
    
    def show_tables_query(self, items):
        seperator = "-" * 24
        result = seperator + "\n"

        database = db.DB()

        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        
            # print table list if exists
            if b"table_list" in database.keys():
                table_list = loads(database.get(bytes("table_list", "utf-8")).decode("utf-8"))
                for table in table_list:
                    result += table + "\n"

            database.close()                                                    # Close database
            result += seperator
            
        except db.DBNoSuchFileError:                                        # NoSuchFile
            result += seperator

        self.result = [False, result]
        return
    
    def describe_query(self, items):
        table_name = items[1].children[0].value.lower()

        database = db.DB()
        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        except db.DBNoSuchFileError:                                        # NoSuchFile
            self.result = [True, "No such table"]
            return

        # NoSuchTable
        if b"table_list" not in database.keys() or table_name not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
            self.result = [True, "No such table"]
            return
        
        column_names = loads(database.get(bytes("table_list/" + table_name + "/column_names", "utf-8")).decode("utf-8"))

        # print formatted table info.
        result = print_table_schema(table_name, column_names, database)

        database.close()                                                    # Close database
        self.result = [False, result]
        return
    
    def explain_query(self, items):
        table_name = items[1].children[0].value.lower()

        database = db.DB()
        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        except db.DBNoSuchFileError:                                        # NoSuchFile
            self.result = [True, "No such table"]
            return

        # NoSuchTable
        if b"table_list" not in database.keys() or table_name not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
            self.result = [True, "No such table"]
            return
        
        column_names = loads(database.get(bytes("table_list/" + table_name + "/column_names", "utf-8")).decode("utf-8"))

        # print formatted table info.
        result = print_table_schema(table_name, column_names, database)

        database.close()                                                    # Close database
        self.result = [False, result]
        return
    
    def select_query(self, items):
        database = db.DB()
        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        except db.DBNoSuchFileError:                                        # SelectDBExistenceError(CustomError)
            self.result = [True, f"Selection has failed: database does not exist"]
            return
        
        # get table names
        table_names = []
        cartesian_column_names = []
        column_in_which_table = dict()

        for table in items[2].children[0].find_data('table_name'):
            table_name = table.children[0].value.lower()

            # SelectTableExistenceError
            if b"table_list" not in database.keys() or table_name not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
                self.result = [True, f"Selection has failed: '{table_name}' does not exist"]
                return
            
            table_names.append(table.children[0].value.lower())

            # put all column names in cartesian_column_names
            column_names = loads(database.get(bytes("table_list/" + table_name + "/column_names", "utf-8")).decode("utf-8"))
            for column_name in column_names:
                if column_in_which_table.get(column_name) != None:
                    column_in_which_table[column_name].append(table_name)
                else:
                    column_in_which_table[column_name] = [table_name]
                cartesian_column_names.append(table_name + "." + column_name)

        # make cartesian product of tables
        cartesian_product = []

        if len(table_names) == 1:                                       # if only one table is given
            table_name = table_names[0]
            record_ids = loads(database.get(bytes("table_list/" + table_name + "/record_ids", "utf-8")).decode("utf-8"))

            if record_ids:                                              # if table is not empty
                for record_id in record_ids:
                    record = loads(database.get(bytes("table_list/" + table_name + "/" + str(record_id), "utf-8")).decode("utf-8"))
                    cartesian_product.append(record)

        elif len(table_names) == 2:                                     # if two tables are given
            table_name1 = table_names[0]
            table_name2 = table_names[1]
            record_ids1 = loads(database.get(bytes("table_list/" + table_name1 + "/record_ids", "utf-8")).decode("utf-8"))
            record_ids2 = loads(database.get(bytes("table_list/" + table_name2 + "/record_ids", "utf-8")).decode("utf-8"))
            
            if record_ids1 and record_ids2:                             # if both tables are not empty
                for record_id1 in record_ids1:
                    for record_id2 in record_ids2:
                        record1 = loads(database.get(bytes("table_list/" + table_name1 + "/" + str(record_id1), "utf-8")).decode("utf-8"))
                        record2 = loads(database.get(bytes("table_list/" + table_name2 + "/" + str(record_id2), "utf-8")).decode("utf-8"))
                        record = {**record1, **record2}
                        cartesian_product.append(record)

        else:                                                           # if three tables are given
            table_name1 = table_names[0]
            table_name2 = table_names[1]
            table_name3 = table_names[2]
            record_ids1 = loads(database.get(bytes("table_list/" + table_name1 + "/record_ids", "utf-8")).decode("utf-8"))
            record_ids2 = loads(database.get(bytes("table_list/" + table_name2 + "/record_ids", "utf-8")).decode("utf-8"))
            record_ids3 = loads(database.get(bytes("table_list/" + table_name3 + "/record_ids", "utf-8")).decode("utf-8"))
            
            if record_ids1 and record_ids2 and record_ids3:             # if all tables are not empty
                for record_id1 in record_ids1:
                    for record_id2 in record_ids2:
                        for record_id3 in record_ids3:
                            record1 = loads(database.get(bytes("table_list/" + table_name1 + "/" + str(record_id1), "utf-8")).decode("utf-8"))
                            record2 = loads(database.get(bytes("table_list/" + table_name2 + "/" + str(record_id2), "utf-8")).decode("utf-8"))
                            record3 = loads(database.get(bytes("table_list/" + table_name3 + "/" + str(record_id3), "utf-8")).decode("utf-8"))
                            record = {**record1, **record2, **record3}
                            cartesian_product.append(record)

        # get where clause
        where_clause = items[2].children[1]
        selected_records = []

        if where_clause is None:                                        # select all records
            selected_records = cartesian_product
        else:                                                           # select records that satisfy where clause
            # determine condition
            boolean_terms = list(where_clause.find_data('boolean_term'))
            boolean_factors = list(where_clause.find_data('boolean_factor'))
            predicate_list = []

            # check logical condition
            log_con = "or" if len(boolean_terms) == 2 else "and" if len(boolean_factors) == 2 else "single"
            
            # parse predicates
            for boolean_factor in boolean_factors:
                expr_negation = True if boolean_factor.children[0] else False
                predicate = list(boolean_factor.find_data('predicate'))

                consequent_table_name = None
                consequent_column_name = None

                is_null_predicate = False
                is_comparison_with_column = False

                if predicate[0].children[0].data == "comparison_predicate":             # comparison predicate
                    first_operand = predicate[0].children[0].children[0]
                    antecedent_table_name = first_operand.children[0].children[0].value.lower() if first_operand.children[0] else None
                    antecedent_column_name = first_operand.children[1].children[0].value.lower()

                    operator = predicate[0].children[0].children[1].value

                    second_operand = predicate[0].children[0].children[2]

                    # case: compare with column
                    if not second_operand.children[0] or second_operand.children[0].data != "comparable_value":
                        is_comparison_with_column = True
                        
                        consequent_table_name = second_operand.children[0].children[0].value.lower() if second_operand.children[0] else None
                        consequent_column_name = second_operand.children[1].children[0].value.lower()

                    # case: compare with value
                    else:
                        comparable_value = second_operand.children[0].children[0].value
                        comparable_value_type = second_operand.children[0].children[0].type.lower()
                
                else:                                                                   # null predicate
                    is_null_predicate = True
                    antecedent_table_name = predicate[0].children[0].children[0].children[0].value.lower() if predicate[0].children[0].children[0] else None
                    antecedent_column_name = predicate[0].children[0].children[1].children[0].value.lower()
                    null_negation = True if predicate[0].children[0].children[2].children[1] else False

                    operator = "is not" if null_negation else "is"

                # WhereTableNotSpecified
                if antecedent_table_name and antecedent_table_name not in table_names:
                    self.result = [True, f"Where clause trying to reference tables which are not specified"]
                    return
                if consequent_table_name and consequent_table_name not in table_names:
                    self.result = [True, f"Where clause trying to reference tables which are not specified"]
                    return
                
                # compare to null with operators other than is/is not
                if consequent_column_name == "null":
                    self.result = [True, "Where clause trying to compare incomparable values"]
                    return
                
                # WhereColumnNotExist
                if not antecedent_table_name and antecedent_column_name not in column_in_which_table:
                    self.result = [True, f"Where clause trying to reference non existing column"]
                    return
                if antecedent_table_name and antecedent_table_name + "." + antecedent_column_name not in cartesian_column_names:
                    self.result = [True, f"Where clause trying to reference non existing column"]
                    return
                if not consequent_table_name and consequent_column_name and consequent_column_name not in column_in_which_table:
                    self.result = [True, f"Where clause trying to reference non existing column"]
                    return
                if consequent_table_name and consequent_table_name + "." + consequent_column_name not in cartesian_column_names:
                    self.result = [True, f"Where clause trying to reference non existing column"]
                    return
                
                # WhereAmbiguousReference
                if not antecedent_table_name and antecedent_column_name in column_in_which_table and len(column_in_which_table[antecedent_column_name]) != 1:
                    self.result = [True, f"Where clause contains ambiguous reference"]
                    return
                if not consequent_table_name and consequent_column_name in column_in_which_table and len(column_in_which_table[consequent_column_name]) != 1:
                    self.result = [True, f"Where clause contains ambiguous reference"]
                    return

                # clarify table name if ambiguous
                antecedent_table_name = antecedent_table_name if antecedent_table_name else column_in_which_table[antecedent_column_name][0]

                antecedent_col_type = database.get(bytes("table_list/" + antecedent_table_name + "/" + antecedent_column_name + "/type", "utf-8")).decode("utf-8")
                
                if antecedent_col_type.startswith("char"):
                    antecedent_col_type = "char"
                
                # WhereIncomparableError
                if not is_null_predicate:

                    if is_comparison_with_column:
                        consequent_table_name = consequent_table_name if consequent_table_name else column_in_which_table[consequent_column_name][0]
                    
                        # compare to null with operators other than is/is not
                        if consequent_column_name == "null":
                            self.result = [True, "Where clause trying to compare incomparable values"]
                            return
                    
                    # classify case whether comparing with column or value
                    consequent_col_type = (
                        database.get(bytes(f"table_list/{consequent_table_name}/{consequent_column_name}/type", "utf-8"))
                        .decode("utf-8") if is_comparison_with_column else comparable_value_type
                    )

                    if consequent_col_type.startswith("char"):
                        consequent_col_type = "char"

                    # check type constraint between antecedent and consequent
                    type_constraints = {"char": {"str", "char"}, "int": {"int"}, "date": {"date"}}

                    if antecedent_col_type not in type_constraints or (consequent_col_type not in type_constraints and consequent_col_type != "str"):
                        self.result = [True, "Where clause trying to compare incomparable values"]
                        return

                    if not type_constraints[antecedent_col_type].intersection({consequent_col_type}):
                        self.result = [True, "Where clause trying to compare incomparable values"]
                        return
                                 
                    if antecedent_col_type == "char":
                        if operator not in ["=", "!="]:
                            self.result = [True, "Where clause trying to compare incomparable values"]
                            return
                        if not is_comparison_with_column:
                            comparable_value = comparable_value[1:-1]                         # remove quotes

                # set consequent      
                if is_null_predicate:
                    consequent = None
                elif is_comparison_with_column:
                    consequent = [consequent_table_name, consequent_col_type, consequent_column_name]
                else:
                    consequent = [comparable_value, comparable_value_type]

                predicate_list.append([expr_negation, [antecedent_table_name, antecedent_column_name, antecedent_col_type], operator, consequent])
            
            for record in cartesian_product:
                if log_con == "single":
                    if check_predicate_condition(predicate_list[0], record):
                        selected_records.append(record)
                elif log_con == "and":
                    if check_predicate_condition(predicate_list[0], record) and check_predicate_condition(predicate_list[1], record):
                        selected_records.append(record)
                else:
                    if check_predicate_condition(predicate_list[0], record) or check_predicate_condition(predicate_list[1], record):
                        selected_records.append(record)

        # get select columns                                          
        if not items[1].children:                                       # select *
            select_column_names = cartesian_column_names
        else:                                                           # select column_name
            select_column_names = []
            for select_column in items[1].find_data('selected_column'):
                selected_table_name = select_column.children[0].children[0].value.lower() if select_column.children[0] else None
                selected_column_name = select_column.children[1].children[0].value.lower()
                
                # SelectColumnResolveError
                if selected_table_name:                                 # if table name is specified
                    candidate_column_name = selected_table_name + "." + selected_column_name
                    if candidate_column_name not in cartesian_column_names:
                        self.result = [True, f"Selection has failed: fail to resolve '{selected_table_name}.{selected_column_name}'"]
                        return
                else:                                                   # if table name is ambiguous
                    column_existence = column_in_which_table.get(selected_column_name)
                    if not column_existence or len(column_existence) > 1:
                        self.result = [True, f"Selection has failed: fail to resolve '{selected_column_name}'"]
                        return
                    
                    # assign proper table name to selected_table_name
                    selected_table_name = column_existence[0]
                    
                select_column_names.append(selected_table_name + "." + selected_column_name)

        # calculate max length of each column(including column name) to align columns
        max_col_width_list = [18 for _ in range(len(select_column_names))]
        for idx, col_name in enumerate(select_column_names):
            max_col_width_list[idx] = max(max_col_width_list[idx], len(col_name))
            for record in selected_records:
                value = record[col_name] if record[col_name] else "NULL"
                max_col_width_list[idx] = max(max_col_width_list[idx], len(str(value)))

        # assign formatted table to self.result
        result = print_table_select(select_column_names, selected_records, max_col_width_list)

        database.close()                                                    # Close database
        self.result = [False, result]
        return
    
    def insert_query(self, items):
        table_name = items[2].children[0].value.lower()
        nullable_values = items[5].find_data('nullable_value')

        database = db.DB()
        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        except db.DBNoSuchFileError:                                        # NoSuchFile
            self.result = [True, "No such table"]
            return

        # NoSuchTable
        if b"table_list" not in database.keys() or table_name not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
            self.result = [True, "No such table"]
            return

        ########################################## things to put in database ##############################################
        # record_dict                (table_list/{table_name}/{uuid})
        # uuid                       (table_list/{table_name}/uuid)
        # record_ids                 (table_list/{table_name}/record_ids)
        ########################################## things to put in database ##############################################

        table_column_names = loads(database.get(bytes("table_list/" + table_name + "/column_names", "utf-8")).decode("utf-8"))
        value_list = []
        value_types = []

        # parse column names from input query if exists
        column_names = []

        if items[3] == None:
            column_names = table_column_names
        else:
            insert_columns = items[3].find_data('column_name')
            for insert_column in insert_columns:
                column_names.append(insert_column.children[0].value.lower())

        # append each parsed value to value list
        for nullable_value in nullable_values:
            if nullable_value.children[0] == "null":                        # store null values as None
                value = None
                value_type = "null"
            else:
                value = nullable_value.children[0].children[0].value
                value_type = nullable_value.children[0].children[0].type.lower()
            value_types.append(value_type)                                  # store value type
            value_list.append(value)                                        # store value

        # InsertTypeMismatchError (column # != value #)
        if len(column_names) != len(value_list):
            self.result = [True, "Insertion has failed: Types are not matched"]
            return

        record_dict = {}

        for idx, col_name in enumerate(column_names):
            # InsertColumnExistenceError
            if col_name not in table_column_names:
                self.result = [True, f"Insertion has failed: '{col_name}' does not exist"]
                return

            col_type = database.get(bytes("table_list/" + table_name + "/" + col_name + "/type", "utf-8")).decode("utf-8")

            # InsertTypeMismatchError (column type != value type)
            if value_types[idx] != "null":
                if col_type.startswith("char"):
                    if value_types[idx] != "str":
                        self.result = [True, "Insertion has failed: Types are not matched"]
                        return
                elif col_type != value_types[idx]:
                    self.result = [True, "Insertion has failed: Types are not matched"]
                    return
            
            # InsertColumnNonNullableError
            elif database.get(bytes("table_list/" + table_name + "/" + col_name + "/is_nullable", "utf-8")).decode("utf-8") == "False":
                self.result = [True, f"Insertion has failed: '{col_name}' is not nullable"]
                return
                
            # InsertReferentialIntegrityError
            if database.get(bytes("table_list/" + table_name + "/" + col_name + "/is_fk", "utf-8")).decode("utf-8") == "True":
                value = value_list[idx]
                foreign_key_values = []
                
                foreign_key_index = loads(database.get(bytes("table_list/" + table_name + "/foreign_key_values", "utf-8")).decode("utf-8")).index(col_name)
                
                referencing_tables = loads(database.get(bytes("table_list/" + table_name + "/referencing_tables", "utf-8")).decode("utf-8"))
                referencing_columns = loads(database.get(bytes("table_list/" + table_name + "/referencing_columns", "utf-8")).decode("utf-8"))
                
                referencing_table_name = referencing_tables[foreign_key_index]
                referencing_column_name = referencing_columns[foreign_key_index]

                ref_record_ids = loads(database.get(bytes("table_list/" + referencing_table_name + "/record_ids", "utf-8")).decode("utf-8"))
                for record_id in ref_record_ids:
                    record = loads(database.get(bytes("table_list/" + referencing_table_name + "/" + str(record_id), "utf-8")).decode("utf-8"))
                    foreign_key_values.append(record[f"{referencing_table_name}.{referencing_column_name}"])

                if value_types[idx] == "str":
                    value = value[1:-1]                                     # Remove quotes
                elif value_types[idx] == "int":
                    value = int(value)

                if value not in foreign_key_values:
                    self.result = [True, "Insertion has failed: Referential integrity violation"]
                    return

            # typecasting value
            if value_types[idx] != "null":
                if col_type.startswith("char"):
                    insert_value = value_list[idx][1:-1]                    # Remove quotes
                    length_constraint = int(col_type[5:-1])
                    if len(insert_value) > length_constraint:               # Truncate value if it exceeds length constraint                                        
                        insert_value = insert_value[:length_constraint]
                elif col_type == "int":
                    insert_value = int(value_list[idx])
                elif col_type == "date":
                    insert_value = value_list[idx]
            else:
                insert_value = None            

            record_dict[table_name + "." + col_name] = insert_value                                   # put each key-value pair in record_dict
        
        primary_keys = loads(database.get(bytes("table_list/" + table_name + "/primary_key", "utf-8")).decode("utf-8"))
        record_ids = loads(database.get(bytes("table_list/" + table_name + "/record_ids", "utf-8")).decode("utf-8"))

        # InsertDuplicatePrimaryKeyError
        for record_id in record_ids:
            record = loads(database.get(bytes("table_list/" + table_name + "/" + str(record_id), "utf-8")).decode("utf-8"))

            is_pk_assigned = []

            for pk in primary_keys:
                if record_dict[table_name + "." + pk] == record[table_name + "." + pk]:
                    is_pk_assigned.append(True)
                else:
                    is_pk_assigned.append(False)

            if len(is_pk_assigned) > 0 and all(is_pk_assigned):
                self.result = [True, "Insertion has failed: Primary key duplication"]
                return
        
        # put null to columns that are not given in input query
        empty_column_names = list(set(table_column_names).difference(set(column_names)))
        if len(empty_column_names) > 0:
            for col_name in empty_column_names:
                # InsertColumnNonNullableError
                if database.get(bytes("table_list/" + table_name + "/" + col_name + "/is_nullable", "utf-8")).decode("utf-8") == "False":
                    self.result = [True, f"Insertion has failed: '{col_name}' is not nullable"]
                    return
                record_dict[table_name + "." + col_name] = None

        # store record_dict in database
        uuid = int(database.get(bytes("table_list/" + table_name + "/uuid", "utf-8")).decode("utf-8"))
        database.put(bytes("table_list/" + table_name + "/" + str(uuid), "utf-8"), bytes(dumps(record_dict), "utf-8"))

        # update record_ids
        record_ids.append(uuid)
        database.put(bytes("table_list/" + table_name + "/record_ids", "utf-8"), bytes(dumps(record_ids), "utf-8"))

        # update uuid
        uuid += 1
        database.put(bytes("table_list/" + table_name + "/uuid", "utf-8"), bytes(str(uuid), "utf-8"))

        database.close()                                                    # Close database
        self.result = [True, "1 row inserted"]
        return
    
    def delete_query(self, items):
        table_name = items[2].children[0].value.lower()
        where_clause = items[3]

        database = db.DB()
        try:
            database.open(self.db_path, None, db.DB_HASH)                   # Open database
        except db.DBNoSuchFileError:                                        # NoSuchFile
            self.result = [True, "No such table"]
            return
        
        # NoSuchTable
        if b"table_list" not in database.keys() or table_name not in loads(database.get(bytes("table_list", "utf-8")).decode("utf-8")):
            self.result = [True, "No such table"]
            return
        
        cursor = database.cursor()                                          # Create cursor
        x = cursor.first()

        delete_uuids = []
        record_ids = loads(database.get(bytes("table_list/" + table_name + "/record_ids", "utf-8")).decode("utf-8"))

        if where_clause is None:                                            # delete all records
            delete_uuids = record_ids

        else:                                                               # delete records that satisfy where clause
            # determine condition
            boolean_terms = list(where_clause.find_data('boolean_term'))
            boolean_factors = list(where_clause.find_data('boolean_factor'))
            predicate_list = []

            # check logical condition
            log_con = "or" if len(boolean_terms) == 2 else "and" if len(boolean_factors) == 2 else "single"

            # parse predicates
            for boolean_factor in boolean_factors:
                expr_negation = True if boolean_factor.children[0] else False
                predicate = list(boolean_factor.find_data('predicate'))

                consequent_table_name = None
                consequent_column_name = None

                is_null_predicate = False
                is_comparison_with_column = False

                if predicate[0].children[0].data == "comparison_predicate":             # comparison predicate
                    first_operand = predicate[0].children[0].children[0]
                    antecedent_table_name = first_operand.children[0].children[0].value.lower() if first_operand.children[0] else None
                    antecedent_column_name = first_operand.children[1].children[0].value.lower()

                    operator = predicate[0].children[0].children[1].value

                    second_operand = predicate[0].children[0].children[2]

                    # case: compare with column
                    if not second_operand.children[0] or second_operand.children[0].data != "comparable_value":
                        is_comparison_with_column = True
                        
                        consequent_table_name = second_operand.children[0].children[0].value.lower() if second_operand.children[0] else None
                        consequent_column_name = second_operand.children[1].children[0].value.lower()

                    # case: compare with value
                    else:
                        comparable_value = second_operand.children[0].children[0].value
                        comparable_value_type = second_operand.children[0].children[0].type.lower()
                
                else:                                                                   # null predicate
                    is_null_predicate = True
                    antecedent_table_name = predicate[0].children[0].children[0].children[0].value.lower() if predicate[0].children[0].children[0] else None
                    antecedent_column_name = predicate[0].children[0].children[1].children[0].value.lower()
                    null_negation = True if predicate[0].children[0].children[2].children[1] else False

                    operator = "is not" if null_negation else "is"

                # WhereTableNotSpecified
                if antecedent_table_name and antecedent_table_name != table_name:
                    self.result = [True, f"Where clause trying to reference tables which are not specified"]
                    return
                if consequent_table_name and consequent_table_name != table_name:
                    self.result = [True, f"Where clause trying to reference tables which are not specified"]
                    return
                
                # if each table name is not specified, set it to table_name
                antecedent_table_name = antecedent_table_name if antecedent_table_name else table_name

                column_names = loads(database.get(bytes("table_list/" + antecedent_table_name + "/column_names", "utf-8")).decode("utf-8"))

                # WhereColumnNotExist & WhereIncomparableError
                if antecedent_column_name not in column_names:
                    self.result = [True, f"Where clause trying to reference non existing column"]
                    return
                if consequent_column_name:
                    if consequent_column_name == "null":                            # 'null' 이라는 컬럼명이 있다면...?
                        self.result = [True, "Where clause trying to compare incomparable values"]
                        return
                    if consequent_column_name not in column_names:
                        self.result = [True, f"Where clause trying to reference non existing column"]
                        return
                
                antecedent_col_type = database.get(bytes("table_list/" + antecedent_table_name + "/" + antecedent_column_name + "/type", "utf-8")).decode("utf-8")
                if antecedent_col_type.startswith("char"):
                    antecedent_col_type = "char"
                
                # WhereIncomparableError
                if not is_null_predicate:

                    # compare to null with operators other than is/is not          
                    if is_comparison_with_column:
                        consequent_table_name = consequent_table_name if consequent_table_name else table_name

                    # classify case whether comparing with column or value
                    consequent_col_type = (
                        database.get(bytes(f"table_list/{consequent_table_name}/{consequent_column_name}/type", "utf-8"))
                        .decode("utf-8") if is_comparison_with_column else comparable_value_type
                    )

                    if consequent_col_type.startswith("char"):
                        consequent_col_type = "char"

                    # check type constraint between antecedent and consequent
                    type_constraints = {"char": {"str", "char"}, "int": {"int"}, "date": {"date"}}

                    if antecedent_col_type not in type_constraints or (consequent_col_type not in type_constraints and consequent_col_type != "str"):
                        self.result = [True, "Where clause trying to compare incomparable values"]
                        return

                    if not type_constraints[antecedent_col_type].intersection({consequent_col_type}):
                        self.result = [True, "Where clause trying to compare incomparable values"]
                        return
                    
                    if antecedent_col_type == "char":
                        if operator not in ["=", "!="]:
                            self.result = [True, "Where clause trying to compare incomparable values"]
                            return
                        if not is_comparison_with_column:
                            comparable_value = comparable_value[1:-1]                         # remove quotes

                # set consequent      
                if is_null_predicate:
                    consequent = None
                elif is_comparison_with_column:
                    consequent = [consequent_table_name, consequent_col_type, consequent_column_name]
                else:
                    consequent = [comparable_value, comparable_value_type]

                predicate_list.append([expr_negation, [antecedent_table_name, antecedent_column_name, antecedent_col_type], operator, consequent])

            # get record_ids that satisfy predicate condition
            for record_id in record_ids:
                record = loads(database.get(bytes("table_list/" + table_name + "/" + str(record_id), "utf-8")).decode("utf-8"))
                if log_con == "single":
                    if check_predicate_condition(predicate_list[0], record):
                        delete_uuids.append(record_id)
                elif log_con == "and":
                    if check_predicate_condition(predicate_list[0], record) and check_predicate_condition(predicate_list[1], record):
                        delete_uuids.append(record_id)
                else:
                    if check_predicate_condition(predicate_list[0], record) or check_predicate_condition(predicate_list[1], record):
                        delete_uuids.append(record_id)

        # DeleteReferentialIntegrityPassed
        referenced_by_tables = []
        referenced_by_columns = []

        while x is not None:
            if x[0].endswith(bytes("/referencing_tables", "utf-8")):
                if table_name in loads(x[1].decode("utf-8")):
                    referenced_by = x[0].decode("utf-8").split("/")[1]
                    referenced_by_tables.append(referenced_by)

                    idx = loads(database.get(bytes("table_list/" + referenced_by + "/referencing_tables", "utf-8")).decode("utf-8")).index(table_name)
                    referenced_by_columns.append(loads(database.get(bytes("table_list/" + referenced_by + "/foreign_key_values", "utf-8")).decode("utf-8"))[idx])
                    referencing_column = loads(database.get(bytes("table_list/" + referenced_by + "/referencing_columns", "utf-8")).decode("utf-8"))[idx]

            x = cursor.next()

        if referenced_by_tables:
            child_table_foreign_key_values = []

            for idx, referenced_by_table in enumerate(referenced_by_tables):
                child_record_ids = loads(database.get(bytes("table_list/" + referenced_by_table + "/record_ids", "utf-8")).decode("utf-8"))
                referenced_by_column = referenced_by_columns[idx]

                for record_id in child_record_ids:
                    record = loads(database.get(bytes("table_list/" + referenced_by_table + "/" + str(record_id), "utf-8")).decode("utf-8"))
                    child_table_foreign_key_values.append(record[f"{referenced_by_table}.{referenced_by_column}"])

            for delete_uuid in delete_uuids:
                record = loads(database.get(bytes("table_list/" + table_name + "/" + str(delete_uuid), "utf-8")).decode("utf-8"))
                if record[f"{table_name}.{referencing_column}"] in child_table_foreign_key_values:
                    self.result = [True, f"{len(delete_uuids)} row(s) are not deleted due to referential integrity"]
                    return

        success_count = 0
        delete_uuids_str = [str(uuid) for uuid in delete_uuids]

        # delete records that satisfy predicate condition
        x  = cursor.first()

        while x is not None:
            leaf_str = str(x[0]).strip("'").split("/")[-1]
            if x[0].startswith(bytes("table_list/" + table_name, "utf-8")) and leaf_str in delete_uuids_str:
                database.delete(x[0])
                success_count += 1
            x = cursor.next()

        # update record_ids information in table metadata
        updated_record_ids = list(set(record_ids).difference(set(delete_uuids)))

        database.put(bytes("table_list/" + table_name + "/record_ids", "utf-8"), bytes(dumps(updated_record_ids), "utf-8"))
        
        cursor.close()                                                      # Close cursor
        database.close()                                                    # Close database
        self.result = [True, f"{success_count} row(s) deleted"]
        return
    
    def update_query(self, items):

        # implement update query
        return
    
def print_table_schema(table_name, column_names, database):
    result = "-" * 65 + "\n"
    result += f"table_name [{table_name}]\n"
    result += f"{'column_name':<20}{'type':<15}{'null':<15}{'key':<15}\n"  # alignment setting

    for column_name in column_names:
        column_type = database.get(bytes("table_list/" + table_name + "/" + column_name + "/type", "utf-8")).decode("utf-8")
        column_is_nullable = database.get(bytes("table_list/" + table_name + "/" + column_name + "/is_nullable", "utf-8")).decode("utf-8")
        column_is_pk = database.get(bytes("table_list/" + table_name + "/" + column_name + "/is_pk", "utf-8")).decode("utf-8")
        column_is_fk = database.get(bytes("table_list/" + table_name + "/" + column_name + "/is_fk", "utf-8")).decode("utf-8")

        null = "Y" if column_is_nullable == "True" else "N"
        if column_is_pk == "True" and column_is_fk == "True":
            key = "PRI/FOR"
        elif column_is_pk == "True":
            key = "PRI"
        elif column_is_fk == "True":
            key = "FOR"
        else:
            key = ""

        result += f"{column_name:<20}{column_type:<15}{null:<15}{key:<15}\n"

    result += "-" * 65
    return result
    
def print_table_select(select_column_names, selected_records, max_col_width_list):
    separator = "".join(f"+{'-' * (width + 2)}" for width in max_col_width_list) + "+"
    result = separator + "\n"

    header = "| " + " | ".join(f"{str(col_name).upper():<{max_col_width_list[idx]}}" for idx, col_name in enumerate(select_column_names)) + " |"
    result += header + "\n"

    result += separator + "\n"

    for record in selected_records:
        row = "| " + " | ".join(f"{record[str(col_name)]:<{max_col_width_list[idx]}}" if record[str(col_name)] else f"{'NULL':<{max_col_width_list[idx]}}" for idx, col_name in enumerate(select_column_names)) + " |"
        result += row + "\n"

    result += separator

    return result
    
def parse_where_clause(where_clause):
    # TODO: integrate where clause processing
    # TODO: consider antecedent for comparable_value
    pass

def check_predicate_condition(predicate, record):                           # Check if predicate condition is satisfied
    # predicate: [expr_negation, [antecedent_table_name, antecedent_column_name, antecedent_column_type], operator, consequent]
    # operator: =, !=, >, <, >=, <=, is, is not
    # consequent: None or [consequent_table_name, consequent_column_type, consequent_column_name] or [comparable_value, comparable_value_type]
    
    antecedent_value = record[predicate[1][0] + "." + predicate[1][1]]      # antecedent value

    if predicate[3] is None:                                                # Null predicate
        result = (predicate[2] == "is") == (antecedent_value is None)

    else:
        if predicate[2] == "=":                                             # Equality predicate
                predicate[2] = "=="

        consequent_value = (                                                # Consequent value
            record[predicate[3][0] + "." + predicate[3][2]]
            if len(predicate[3]) == 3
            else predicate[3][0]
        )

        if antecedent_value is None or consequent_value is None:            # comparison with null
            return False
        
        # typecasting
        if predicate[1][2] == "char" or predicate[3][1] == "char":
            antecedent_value, consequent_value = f"'{antecedent_value}'", f"'{consequent_value}'"

        if predicate[1][2] == "date" or predicate[3][1] == "date":
            antecedent_value, consequent_value = f"'{antecedent_value}'", f"'{consequent_value}'"

        result = eval(f"{antecedent_value} {predicate[2]} {consequent_value}")
    
    return result if not predicate[0] else not result 
    
def print_with_prompt(output):                      # Show prompt message with student ID
    if type(output[1]) != str:
        output[1] = str(output[1])

    print("DB_2018-15001> " + output[1] if output[0] else output[1])                # Show prompt message

def split_input_include_semicolon(input_queries):   # Split input queries by semicolon
    query_list = []
    query = ""
    for char in input_queries:
        if char in ['\n', '\t', '\r']:              # Ignore newline, tab, carriage return
            continue
        query += char
        if char == ';':
            query_list.append(query.strip())        # Append query to query list when semicolon is found
            query = ""
    return query_list

def input_until_semicolon_followed_enter():         # Input queries
    input_lines = []
    print("DB_2018-15001>", end=" ")                # Show prompt message  

    while True:                                     # Loop until semicolon followed by enter is found
        line = input()
        input_lines.append(line)

        if line.strip().endswith(';'):
            break

    return ' '.join(input_lines)                    # Return input queries as a string

if __name__ == "__main__":                                          # Main function to execute parser
    db_path = "myDB.db"                                             # Set database path

    with open('grammar.lark') as file:                              # Open grammar file
        sql_parser = Lark(file.read(), start="command", lexer="dynamic")

    while(1):                                                       # Loop until exit command is given
        input_queries = input_until_semicolon_followed_enter()      # Input queries
        query_list = split_input_include_semicolon(input_queries)   # Split input queries by semicolon
        result_seq = []                                             # List to store query results

        for query in query_list:                                    # Parse each query
            if query[:-1].strip() == "exit":                        # Exit command
                exit(0)                                             # Exit program
            try:
                output = sql_parser.parse(query)                    # Parse query with lark parser
                transformer = MyTransformer(db_path)                # Create MyTransformer class
                transformer.transform(output)                       # Transform parse tree with MyTransformer class
                result = transformer.result                         # Get result from MyTransformer class
                if result != None:
                    result_seq.append(result)
            except UnexpectedCharacters:                            # Lark grammar error
                result_seq.append([True, "Syntax error"])                   # Print error message when syntax error occurs
                break
            except Exception as e:                                  # Print error message when unexpected error occurs
                result_seq.append([True, f"Unexpected: {e}"])
                break
            
        for result in result_seq:                                   # Print each result
            if result != None:
                print_with_prompt(result)
