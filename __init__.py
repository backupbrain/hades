import sqlite3
import copy

class_structures = {}


class DatabaseObject:
    """Database Object."""

    in_verbose_mode = False
    database_manager = None
    primary_key = None
    columns = {}
    data = {}
    is_loaded = False
    is_structure_loaded = False
    query = {
        'command': None,
        'where': [],
        'limit': (0, 1),
        'order_by': {}
    }
    reserved_column_names = []

    def __init__(self, database_manager):
        """Initialize the object."""
        self.database_manager = database_manager
        self.reserved_column_names = dir(self)

    def reset_query(self):
        """Reset the current query parameters."""
        self.query = {
            'command': None,
            'where': [],
            'limit': (0, 1),
            'order_by': {}
        }

    def get_obj(self, *args, **kwargs):
        """fetch."""
        if self.is_structure_loaded is False:
            self.load_structure(self.database_manager.fetch_object_structure(self))
        self.reset_query
        self.query['command'] = 'select'
        for key, value in kwargs.items():
            self.query['where'].append({'column': key, 'equivalence': '=', 'value': value})
        self.query['limit'] = (0, 1)
        query = self.build_query()
        print(query)
        cursor = self.database_manager.query(query)
        results = self.database_manager.fetch_query_results(cursor)
        if len(results) == 1:
            for index, column in value:
                if hasattr(self, column):
                    setattr(self, column, value)
        print(results)

    @classmethod
    def get(cls, database_manager, *args, **kwargs):
        """fetch."""
        object_name = database_manager.camel_to_snake(cls.__name__)
        if object_name not in class_structures:
            object_structure = database_manager.fetch_object_structure(object_name)
            class_structures[object_name] = object_structure

        obj = cls(database_manager)
        obj.load_structure(class_structures[object_name])
        obj.reset_query()
        obj.query['command'] = 'select'
        for key, value in kwargs.items():
            obj.query['where'].append({'column': key, 'equivalence': '=', 'value': value})
        obj.query['limit'] = (0, 1)
        query = obj.build_query()
        print(query)
        cursor = obj.database_manager.query(query)
        results = obj.database_manager.fetch_query_results(cursor)
        print(results)
        num_results = len(results)
        print("num results: {}".format(num_results))
        if num_results == 0:
            raise obj.NoneFoundException
        elif num_results == 1:
            result = results[0]
            obj.load_data(result)
        else:
            raise obj.MultipleFoundException
        print(obj)
        return obj

    def load_data(self, data):
        """Load data from a database query."""
        obj = self
        index = 0
        for column in obj.columns:
            if column in obj.reserved_column_names:
                obj.say(
                    "WARNING: Use of column name {} conflicts with class method".format(
                        column
                    ),
                    important=True
                )
            value = data[index]
            setattr(obj, column, value)
            index += 1

    def load_structure(self, columns):
        """Load structure."""
        for index, name, data_type, notnull, default_value, pk in columns:
            self.columns[name] = {
                'notnull': notnull,
                'data_type': data_type,
                'default_value': default_value
            }
        self.is_structure_loaded

    def build_query(self):
        """Build a new query."""
        command = self.query['command']
        table_name = self.database_manager.camel_to_snake(self.__class__.__name__)

        where_conditions = []
        for row in self.query['where']:
            print(row)
            where_conditions.append('`{}`{}"{}"'.format(
                row['column'],
                row['equivalence'],
                row['value']
            ))
        where = ""
        if len(where_conditions) > 0:
            wheres = " and ".join(where_conditions)
            where = "WHERE ({})".format(wheres)
        limit = "LIMIT {}".format(", ".join([str(self.query['limit'][0]), str(self.query['limit'][1])]))
        command_lower = command.lower()
        if command_lower == "select":
            query = "{} * FROM `{}` {} {}".format(
                command,
                table_name,
                where,
                limit
            )
        elif command_lower == 'delete':
            query = "{} FROM `{}` {}".format(
                command,
                table_name,
                where
            )
        elif command_lower == 'insert':
            column_list = []  # ", ".join(['`{}`'.format(column) for column in self.columns])
            value_list = []
            for column in self.columns:
                if column != 'id':
                    if hasattr(self, column):
                        print(column, getattr(self, column))
                        column_list.append('`{}`'.format(column))
                        value_list.append("'{}'".format(getattr(self, column)))
            columns = ", ".join(column_list)
            values = ", ".join(value_list)
            print(values)
            query = "{} INTO `{}` ({}) VALUES ({})".format(
                command,
                table_name,
                columns,
                values
            )
        elif command_lower == "update":
            # column_list = []  # ", ".join(['`{}`'.format(column) for column in self.columns])
            set_list = []
            for column in self.columns:
                if column != 'id':
                    if hasattr(self, column):
                        # column_list.append("`{}`".format(column))
                        set_list.append("`{}`='{}'".format(column, getattr(self, column)))
            # columns = ", ".format(column_list)
            set_info = ", ".join(set_list)
            query = "{} `{}` SET {} WHERE (id={})".format(
                command,
                table_name,
                set_info,
                self.id
            )

        return query

    def save(self):
        """Create/update."""
        if hasattr(self, 'id'):
            if self.id is None:
                self.create()
            else:
                self.update()
        else:
            self.create()

    def update(self):
        """Update existing record."""
        self.reset_query()
        self.query['command'] = 'update'
        for column in self.columns:
            self.query['where'].append({'column': column, 'equivalence': '=', 'value': getattr(self, column)})
        query = self.build_query()
        print(query)
        cursor = self.database_manager.query(query)
        # output = self.fetch_query_results(cursor)
        # print(output)

    def create(self):
        """Update existing record."""
        self.reset_query()
        self.query['command'] = 'insert'
        for column in self.columns:
            if hasattr(self, column):
                self.query['where'].append({'column': column, 'equivalence': '=', 'value': getattr(self, column)})
        query = self.build_query()
        print(query)
        cursor = self.database_manager.query(query)
        self.id = cursor.lastrowid

    def say(self, message, important=False):
        """Print debugging messages."""
        if self.in_verbose_mode is True or important is True:
            class_name = self.__class__.__name__
            print('[{}] {}'.format(class_name, message))

    class Curator:
        """Curate the objects."""

        @staticmethod
        def fetch(cls, database_manager, *args, **kwargs):
            """Fetch multiple rows."""
            print(cls.__name__)
            template_object = cls(database_manager)
            template_object.query['command'] = 'select'
            for key, value in kwargs.items():
                template_object.query['where'].append({'column': key, 'equivalence': '=', 'value': value})
            query = template_object.build_query()
            cursor = database_manager.query(query)
            results = database_manager.fetch_query_results(cursor)
            output = []
            for result in results:
                obj = copy.copy(template_object)
                obj.load_data(result)
                output.append(obj)
            print(output)
            return output

        @staticmethod
        def delete(cls, database_manager, *args, **kwargs):
            """Fetch multiple rows."""
            print(cls.__name__)
            template_object = cls(database_manager)
            template_object.query['command'] = 'delete'
            for key, value in kwargs.items():
                template_object.query['where'].append({'column': key, 'equivalence': '=', 'value': value})
            query = template_object.build_query()
            cursor = database_manager.query(query)
            results = database_manager.fetch_query_results(cursor)
            output = []
            for result in results:
                obj = copy.copy(template_object)
                obj.load_data(result)
                output.append(obj)
            print(output)
            return output

    class NoneFoundException(Exception):
        """No items found."""

        pass

    class MultipleFoundException(Exception):
        """Multiple items found."""

        pass


class DatabaseManager:
    """Database Manager."""

    in_verbose_mode = False

    DB_TYPE_SQLITE3 = 'sqlite3'
    DB_TYPE_MYSQL = 'mysql'
    DB_TYPES = [
        DB_TYPE_SQLITE3,
        DB_TYPE_MYSQL
    ]

    type = None
    settings = None
    connection = None
    is_connected = False

    def __init__(self, type, settings, in_verbose_mode=False):
        """Initialize the database."""
        self.in_verbose_mode = in_verbose_mode
        self.say("In debug mode")
        self.type = type
        self.settings = settings

    def get_connection(self):
        """Connect to the database."""
        if self.connection is None:
            if self.type == self.DB_TYPE_SQLITE3:
                self.connection = sqlite3.connect(self.settings['file'])
        return self.connection

    def fetch_object_structure(self, obj):
        """Fetch object structure."""
        if isinstance(obj, dict):
            object_name = self.camel_to_snake(obj.__class__.__name__)
        else:
            object_name = self.camel_to_snake(obj)
        if self.type == self.DB_TYPE_MYSQL:
            query = "SHOW COLUMNS in `{}'".format(object_name)
        elif self.type == self.DB_TYPE_SQLITE3:
            query = "PRAGMA table_info(`{}`);".format(object_name)
        cursor = self.query(query)
        return self.fetch_queryset(cursor)

    @staticmethod
    def camel_to_snake(str):
        """Convert camel case to snake case."""
        res = [str[0].lower()]
        for c in str[1:]:
            if c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
                res.append('_')
                res.append(c.lower())
            else:
                res.append(c)
        return ''.join(res)

    @staticmethod
    def snake_to_camel(word):
        """Convert snake case to camel case."""
        return ''.join(x.capitalize() or '_' for x in word.split('_'))

    def fetch_query_results(self, cursor):
        """Fetch query results."""
        rows = cursor.fetchall()
        print(rows)
        for row in rows:
            print(row)
        return rows

    def query(self, query):
        """Route the query."""
        self.say(query)
        cursor = self.get_connection().cursor()
        command = query[:6].upper()
        if command == "INSERT":
            return self.insert(query, cursor)
        elif command == "UPDATE":
            return self.update(query, cursor)
        elif command == "DELETE":
            return self.update(query, cursor)
        else:
            return self.select(query, cursor)

    def select(self, query, cursor):
        """Select and Show queries."""
        cursor.execute(query)
        return cursor

    def insert(self, query, cursor):
        """Insert."""
        cursor.execute(query)
        # TODO: get resulting ID
        return cursor

    def update(self, query, cursor):
        """Insert."""
        cursor.execute(query)
        # TODO: get number of affected rows
        return cursor

    def delete(self, query, cursor):
        """Insert."""
        cursor.execute(query)
        # TODO: get number of affected rows
        return cursor

    def fetch_queryset(self, cursor):
        """Fetch the queryset."""
        result = cursor.fetchall()
        print(result)
        return result

    def say(self, message):
        """Print debugging messages."""
        if self.in_verbose_mode is True:
            class_name = self.__class__.__name__
            print('[{}] {}'.format(class_name, message))
