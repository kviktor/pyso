import sqlite3

# TODO
# ForeignKey
# order_by
# ManyToMany
# distinct


class Field:
    def __init__(self, nullable=False, default=None):
        self.nullable = nullable
        self.default = default

    def to_db(self, value):
        if value is None:
            return "null"

        return value


class TextField(Field):
    SQLITE_TYPE = "TEXT"


class IntegerField(Field):
    SQLITE_TYPE = "INTEGER"


class FloatField(Field):
    SQLITE_TYPE = "REAL"


class ModelMeta(type):
    def __new__(cls, name, bases, dct):
        _meta = {}
        _meta['table_name'] = name.lower()
        _meta['fields'] = {}
        for k, v in dct.iteritems():
            _meta['fields']
            if isinstance(v, Field):
                _meta['fields'][k] = v

        dct['_meta'] = _meta
        return type.__new__(cls, name, bases, dct)


class Model:
    __metaclass__ = ModelMeta

    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            if key not in self._meta['fields'].keys():
                raise ValueError("No field named '%s'" % key)

        for key, field in self._meta['fields'].iteritems():
            if key in kwargs.keys():
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, field.default)

    @classmethod
    def _from_row(cls, row):
        obj = cls(**{
            k: row[k]
            for k in cls._meta['fields'].keys()
        })
        setattr(obj, "pk", row['rowid'])
        return obj

    @property
    def _saved(self):
        return getattr(self, "pk", None) is not None

    def save(self):
        fields = self._meta['fields']
        for key, field in fields.iteritems():
            if not field.nullable and getattr(self, key) is None:
                raise ValueError("'%s' cannot be null" % key)

        if self._saved:
            sql = "UPDATE %s SET %s WHERE rowid=%d"
            self.connection.execute(sql % (
                self._meta['table_name'],
                ",".join(["%s=?" % key for key in fields.keys()]),
                self.pk
            ), [f.to_db(getattr(self, k)) for k, f in fields.iteritems()])
        else:
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (
                self._meta['table_name'],
                ",".join(self._meta['fields'].keys()),
                ",".join(["?"] * len(self._meta['fields'].keys()))
            )
            cursor = self.connection.cursor()
            cursor.execute(sql, [
                getattr(self, key)
                for key in self._meta['fields'].keys()
            ])
            setattr(self, "pk", cursor.lastrowid)

    @classmethod
    def create(cls, **kwargs):
        obj = cls(**kwargs)
        obj.save()
        return obj

    @classmethod
    def filter(cls, **kwargs):
        return QuerySet(cls, filters=Operation.convert_kwargs(**kwargs))

    @classmethod
    def get(cls, **kwargs):
        return QuerySet(cls).get(**kwargs)

    @classmethod
    def exclude(cls, **kwargs):
        return QuerySet(cls, filters=None,
                        excludes=Operation.convert_kwargs(**kwargs))

    @classmethod
    def all(cls):
        return cls.filter()

    @classmethod
    def set_connection(cls, string):
        cls.connection = sqlite3.connect(string, isolation_level=None)
        cls.connection.row_factory = sqlite3.Row

    class DoesNotExist(Exception):
        pass

    class MultipleObjectsReturned(Exception):
        pass


class Operation:
    available = [
        "startswith", "endswith", "contains", "gt", "lt", "gte", "lte",
        "ne",
    ]

    def __init__(self, field, method, value):
        self.field = field
        self.method = method
        self.value = value

    @classmethod
    def convert_kwargs(cls, **kwargs):
        ops = []
        for key, value in kwargs.iteritems():
            if "__" in key:
                parts = key.split("__")
                op = parts[-1]
                if op not in Operation.available:
                    raise ValueError("Operation '%s' is not supported" % op)

                ops.append(Operation(parts[0], op, value))
            else:
                ops.append(Operation(key, None, value))
        return ops

    def convert(self):
        if not self.method:
            if self.value:
                return "%s=?" % self.field, self.value
            else:
                return "%s IS NULL" % self.field, None
        else:
            return getattr(self, self.method)()

    def startswith(self):
        return "%s LIKE ?" % self.field, "%s%%" % self.value

    def endswith(self):
        return "%s LIKE ?" % self.field, "%%%s" % self.value

    def contains(self):
        return "%s LIKE ?" % self.field, "%%%s%%" % self.value

    def gt(self):
        return "%s > ?" % self.field, self.value

    def gte(self):
        return "%s >= ?" % self.field, self.value

    def lt(self):
        return "%s < ?" % self.field, self.value

    def lte(self):
        return "%s <= ?" % self.field, self.value

    def ne(self):
        return "%s <> ?" % self.field, self.value


class QuerySet:
    def __init__(self, model, filters=None, excludes=None):
        self.model = model
        self.filters = filters or []
        self.excludes = excludes or []
        self._cache = None

        self._clean_filters_and_excludes()

    def _clean_filters_and_excludes(self):
        self._clean_params(self.filters)
        self._clean_params(self.excludes)

    def _clean_params(self, params):
        fields = self.model._meta['fields'].keys()
        for p in params:
            if p.field in ("pk", "id", "rowid"):
                p.field = "rowid"
            else:
                if p.field not in fields:
                    raise ValueError("'%s' is not a field, available "
                                     "options are: %s" % (
                                         p.field, ", ".join(fields)))

    def __repr__(self):
        self._fetch_all()
        return "[%s]" % ", ".join(p.__repr__() for p in self._cache)

    def __iter__(self):
        self._fetch_all()
        return iter(self._cache)

    def __getitem__(self, idx):
        self._fetch_all()
        return self._cache[idx]

    def __len__(self):
        self._fetch_all()
        return len(self._cache)

    def _build_where(self):
        sql = ""
        params = []
        f_extra = []
        e_extra = []

        for f in self.filters:
            o_sql, o_params = f.convert()
            f_extra.append(o_sql)
            if o_params:
                params.append(o_params)

        for e in self.excludes:
            o_sql, o_params = e.convert()
            e_extra.append(o_sql)
            if o_params:
                params.append(o_params)

        if self.filters or self.excludes:
            sql += " WHERE "
        sql += " AND ".join(f_extra)
        if self.filters and self.excludes:
            sql += " AND "
        if self.excludes:
            sql += " NOT (%s)" % " OR ".join(e_extra)

        return sql, params

    def _fetch_all(self):
        if self._cache:
            return

        sql = "SELECT rowid, * FROM %s " % self.model._meta['table_name']
        where, params = self._build_where()
        sql += where

        self._cache = [
            self.model._from_row(r)
            for r in self.model.connection.execute(sql, params).fetchall()
        ]

    def delete(self):
        sql = "DELETE FROM %s" % self.model._meta['table_name']
        where, params = self._build_where()
        sql += where
        self.model.connection.execute(sql, params)

    def filter(self, **kwargs):
        return QuerySet(self.model,
                        self.filters + Operation.convert_kwargs(**kwargs),
                        self.excludes)

    def exclude(self, **kwargs):
        return QuerySet(self.model,
                        self.filters,
                        self.excludes + Operation.convert_kwargs(**kwargs))

    def get(self, **kwargs):
        qs = QuerySet(self.model,
                      self.filters + Operation.convert_kwargs(**kwargs),
                      self.excludes)
        if qs.count() < 1:
            raise self.model.DoesNotExist
        if qs.count() > 1:
            raise self.model.MultipleObjectsReturned
        return qs[0]

    def count(self):
        return self.__len__()


def create_table(cls):
    if cls.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND "
        "name='%s'" % cls._meta['table_name']
    ).fetchone():
        # TODO
        # cls.connection.execute("DELETE FROM post")
        print(cls.all())
        print("Table '%s' already exists." % cls._meta['table_name'])
    else:
        cls.connection.execute("CREATE TABLE %s(%s);" % (
            cls._meta['table_name'],
            ", ".join([
                "%s %s" % (key, value.SQLITE_TYPE)
                for key, value in cls._meta['fields'].iteritems()
            ])
        ))


def scan_models():
    pass
