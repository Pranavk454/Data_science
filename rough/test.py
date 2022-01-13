table_name = "csv"
query_string1 = 'TRUNCATE TABLE ' + table_name + ';'
query_string1 += """
                        INSERT INTO
                        """
query_string1 += ' ' + table_name
query_string1 += " SELECT *,split(_FILE_NAME,'/')[ordinal(%s)] LOAD_FILENAME FROM"
query_string1 += ' ' + table_name + '_EXT;'
query_string1 += 'DROP TABLE ' + table_name + '_EXT;'

print(query_string1)