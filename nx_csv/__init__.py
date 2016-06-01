"""Miscellaneous CSV readers/writers.

Note: a lot of the doctests are also exercising the functions in rdr_wrtr as
they are difficult to doctest on their own. As such, some functionality is
skipped as it is duplicated in other tests."""
import io
import csv
from csv import (
    QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONE, QUOTE_NONNUMERIC, Dialect,
    register_dialect)
from nx_rdr_wrtr import rdr_utils
from nx_rdr_wrtr import wrtr_utils
import nx_fsutils
import nx_tempfile


# In keeping with the CSV dialect naming convention
class pipe(Dialect): # pylint: disable=invalid-name,too-few-public-methods
    "Common dialect - Excel with pipes instead of commas"
    delimiter = '|'
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    quoting = QUOTE_MINIMAL
register_dialect('pipe', pipe)


def reader_gen(src, *, dialect='excel', delimiter=None, quotechar=None,
               doublequote=None, quoting=None, strict=True, encoding=None,
               errors=None, handler=None):
    """
    Base reader for CSV files.

    >>> f = io.StringIO('a,b\\n1,2\\n\\n3,4\\n')
    >>> list(reader_gen(f))
    [Line: 1 ['a', 'b'], Line: 2 ['1', '2'], Line: 3 [], Line: 4 ['3', '4']]

    >>> f = io.StringIO('a,b\\n1,2\\n\\n3,4\\n')
    >>> list(reader_gen(f, handler=lambda x: x if x.line_num == 2 else None))
    [Line: 2 ['1', '2']]

    >>> f = io.StringIO('a|b\\n1|2\\n3|4\\n')
    >>> list(reader_gen(f, delimiter='|'))
    [Line: 1 ['a', 'b'], Line: 2 ['1', '2'], Line: 3 ['3', '4']]
    """
    class Line(rdr_utils.MutableLine, list):
        "List line class."
        pass
    kwargs = {'dialect': dialect, 'strict': strict}
    if delimiter is not None:
        kwargs['delimiter'] = delimiter
    if quotechar is not None:
        kwargs['quotechar'] = quotechar
    if doublequote is not None:
        kwargs['doublequote'] = doublequote
    if quoting is not None:
        kwargs['quoting'] = quoting
    with nx_fsutils.open_src(src, 'rt', encoding=encoding, errors=errors,
                             newline='') as fobj:
        rdr = csv.reader(fobj, **kwargs)
        def _(rdr):
            last_end = 0
            for rec in rdr:
                result, last_end = Line(rec, last_end + 1), rdr.line_num
                yield result
        rdr = _(rdr)
        if handler:
            rdr = (x for x in map(handler, rdr) if x is not None) # pylint: disable=bad-builtin
        yield from rdr


def reader(src, *, dialect='excel', delimiter=None, quotechar=None,
           doublequote=None, quoting=None, strict=True, encoding=None,
           errors=None, handler=None, leading_ws=False, trailing_ws=False,
           ignore_blanks=True):
    """CSV list reader.

    >>> f = io.StringIO('a,b\\n1, 2 \\n\\n3,4\\n')
    >>> list(reader(f))
    [Line: 1 ['a', 'b'], Line: 2 ['1', '2'], Line: 4 ['3', '4']]

    >>> f = io.StringIO('a,b\\n1, 2 \\n\\n3,4\\n')
    >>> list(reader(f, ignore_blanks=False))
    [Line: 1 ['a', 'b'], Line: 2 ['1', '2'], Line: 3 [], Line: 4 ['3', '4']]

    >>> f = io.StringIO('a,b\\n1, 2 \\n\\n3,4\\n')
    >>> list(reader(f, handler=lambda x: x if x.line_num == 2 else None))
    [Line: 2 ['1', '2']]

    >>> f = io.StringIO('a,b\\n1, 2 \\n\\n3,4\\n')
    >>> list(reader(f, leading_ws=True))
    [Line: 1 ['a', 'b'], Line: 2 ['1', ' 2'], Line: 4 ['3', '4']]

    >>> f = io.StringIO('a,b\\n1, 2 \\n\\n3,4\\n')
    >>> list(reader(f, trailing_ws=True))
    [Line: 1 ['a', 'b'], Line: 2 ['1', '2 '], Line: 4 ['3', '4']]
    """
    rdr = reader_gen(src, dialect=dialect, delimiter=delimiter,
                     quotechar=quotechar, doublequote=doublequote,
                     quoting=quoting, strict=strict, encoding=encoding,
                     errors=errors, handler=handler)
    try:
        yield from rdr_utils.list_reader(rdr, leading_ws=leading_ws,
                                         trailing_ws=trailing_ws,
                                         ignore_blanks=ignore_blanks)
    finally:
        rdr.close()


# pylint: disable=too-many-locals
def dict_reader(src, fields=None, *, dialect='excel', delimiter=None,
                quotechar=None, doublequote=None, quoting=None, strict=True,
                encoding=None, errors=None, handler=None, raw_handler=None,
                leading_ws=False, trailing_ws=False, ignore_blanks=True,
                rest_key=None, rest_val=None, ignore_rows_with_fields=True,
                field_rename=None):
    """CSV dictionary reader.

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4\\n')
    >>> list(dict_reader(f)) == [dict(a='1', b='2'), dict(a='3', b='4')]
    True

    >>> f = io.StringIO('1,2\\na,b\\n3,4\\n')
    >>> (list(dict_reader(f, ('a', 'b'))) ==
    ...  [dict(a='1', b='2'), dict(a='3', b='4')])
    True

    >>> f = io.StringIO('1,2\\na,b\\n3,4\\n')
    >>> (list(dict_reader(f, ('x', 'y'))) ==
    ...  [dict(x='1', y='2'), dict(x='a', y='b'), dict(x='3', y='4')])
    True

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4\\n')
    >>> (list(dict_reader(f, ignore_rows_with_fields=False)) ==
    ...  [dict(a='1', b='2'), dict(a='a', b='b'), dict(a='3', b='4')])
    True

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4\\n')
    >>> (list(dict_reader(f,
    ...                   handler=lambda x: x if x.line_num != 2 else None)) ==
    ...  [dict(a='3', b='4')])
    True

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3\\n')
    >>> list(dict_reader(f))
    Traceback (most recent call last):
        ...
    ValueError: ('insufficient fields', 4, 2, 1)

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3\\n')
    >>> (list(dict_reader(f, rest_val='7')) ==
    ...  [dict(a='1', b='2'), dict(a='3', b='7')])
    True

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4,5\\n')
    >>> list(dict_reader(f))
    Traceback (most recent call last):
        ...
    ValueError: ('too many fields', 4, 2, 3)

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4,5\\n')
    >>> (list(dict_reader(f, rest_key='a')) ==
    ...  [dict(a='1', b='2'), dict(a='3', b='4', a0='5')])
    True

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4\\n')
    >>> (list(dict_reader(f, field_rename={'b': 'c'})) ==
    ...  [dict(a='1', c='2'), dict(a='3', c='4')])
    True
    """
    rdr = reader_gen(src, dialect=dialect, delimiter=delimiter,
                     quotechar=quotechar, doublequote=doublequote,
                     quoting=quoting, strict=strict, encoding=encoding,
                     errors=errors, handler=raw_handler)
    try:
        yield from rdr_utils.dict_reader(rdr, fields=fields, handler=handler,
                                         leading_ws=leading_ws,
                                         trailing_ws=trailing_ws,
                                         ignore_blanks=ignore_blanks,
                                         rest_key=rest_key, rest_val=rest_val,
                                         ignore_rows_with_fields=\
                                             ignore_rows_with_fields,
                                         field_rename=field_rename)
    finally:
        rdr.close()


# pylint: disable=too-many-locals
def obj_reader(src, ctor, fields=None, *, dialect='excel', delimiter=None,
               quotechar=None, doublequote=None, quoting=None, strict=True,
               encoding=None, errors=None, handler=None, raw_handler=None,
               leading_ws=False, trailing_ws=False, ignore_blanks=True,
               rest_key=None, rest_val=None, ignore_rows_with_fields=True,
               field_rename=None):
    """CSV object reader.

    >>> class A: pass

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4\\n')
    >>> r = list(obj_reader(f, A))
    >>> (len(r) == 2 and r[0].a == '1' and r[0].b == '2' and r[1].a == '3' and
    ...  r[1].b == '4')
    True

    >>> f = io.StringIO('a,b\\n1,2\\na,b\\n3,4\\n')
    >>> r = list(obj_reader(f, A, ('a', 'b')))
    >>> (len(r) == 2 and r[0].a == '1' and r[0].b == '2' and r[1].a == '3' and
    ...  r[1].b == '4')
    True

    >>> f = io.StringIO('1,2\\na,b\\n3,4\\n')
    >>> r = list(obj_reader(f, A, ('x', 'y')))
    >>> (len(r) == 3 and r[0].x == '1' and r[0].y == '2' and r[1].x == 'a' and
    ...  r[1].y == 'b' and r[2].x == '3' and r[2].y == '4')
    True
    """
    rdr = reader_gen(src, dialect=dialect, delimiter=delimiter,
                     quotechar=quotechar, doublequote=doublequote,
                     quoting=quoting, strict=strict, encoding=encoding,
                     errors=errors, handler=raw_handler)
    try:
        yield from rdr_utils.obj_reader(rdr, ctor, fields=fields,
                                        handler=handler,
                                        leading_ws=leading_ws,
                                        trailing_ws=trailing_ws,
                                        ignore_blanks=ignore_blanks,
                                        rest_key=rest_key, rest_val=rest_val,
                                        ignore_rows_with_fields=\
                                            ignore_rows_with_fields,
                                        field_rename=field_rename)
    finally:
        rdr.close()


class Writer:
    "CSV list writer."
    _closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        "Close underlying data."
        if not self._closed:
            self._closed = True
            self._fobj, fobj = None, self._fobj
            self._obj = None
            if self._managed:
                fobj.close()

    def write(self, data):
        "Write data."
        handler = self.handler
        if handler:
            data = handler(data)
            if data is None:
                return
        self._obj.writerow(data)

    def __init__(self, src, mode='wt', *, dialect='excel', delimiter=None,
                 quotechar=None, doublequote=None, quoting=None,
                 lineterminator=None, strict=True, encoding=None,
                 errors=None, handler=None):
        self.handler = handler
        kwargs = {'dialect': dialect, 'strict': strict}
        if delimiter is not None:
            kwargs['delimiter'] = delimiter
        if quotechar is not None:
            kwargs['quotechar'] = quotechar
        if doublequote is not None:
            kwargs['doublequote'] = doublequote
        if quoting is not None:
            kwargs['quoting'] = quoting
        if lineterminator is not None:
            kwargs['lineterminator'] = lineterminator
        self._fobj, self._managed = nx_fsutils.open_src_file(src, mode,
                                                             encoding=encoding,
                                                             errors=errors,
                                                             newline='')
        try:
            self._obj = csv.writer(self._fobj, **kwargs)
        except:
            if self._managed:
                self._fobj.close()
            raise
        self._closed = False


# Class is abstract
class _MapWriter:                 # pylint: disable=too-few-public-methods
    # pylint: disable=too-many-arguments
    def __init__(self, src, fields, mode, encoding, errors, **kwargs):
        self.fields = [getattr(col, 'name', col) for col in fields]
        self._field_set = set(self.fields)
        self._wrtr = Writer(src, mode, encoding=encoding, errors=errors,
                            handler=None, **kwargs)
        try:
            if self.minimize:           # pylint: disable=no-member
                self._fields_used = set(self.include or {}) # pylint: disable=no-member
                extras = self._fields_used - self._field_set
                if extras:
                    raise ValueError('include specifies fields not specified',
                                     extras)
                self._fobj = \
                    nx_tempfile.SpooledTemporaryFile(10485760, 'xt+',
                                                     encoding=encoding,
                                                     errors=errors)
        except:
            self._wrtr.close()
            raise


class DictWriter(wrtr_utils.HeaderMixin, wrtr_utils.DictWriterMixin,
                 _MapWriter):
    "CSV dictionary writer."
    def __del__(self):
        self.close()

    def __init__(self, src, fields, mode='wt', *, dialect='excel',
                 delimiter=None, quotechar=None, doublequote=None,
                 quoting=None, lineterminator=None, strict=True, encoding=None,
                 errors=None, handler=None, minimize=False, include=None,
                 rest_val=None, extras_action='raise'):
        self.handler = handler
        self.minimize = minimize
        self.include = include
        self.rest_val = rest_val
        self.extras_action = extras_action
        super().__init__(src, fields, mode, encoding, errors, dialect=dialect,
                         delimiter=delimiter, quotechar=quotechar,
                         doublequote=doublequote, quoting=quoting,
                         lineterminator=lineterminator, strict=strict)
        self._closed = False


class ObjWriter(wrtr_utils.HeaderMixin, wrtr_utils.ObjWriterMixin, _MapWriter):
    "CSV object writer."
    def __del__(self):
        self.close()

    def __init__(self, src, fields, mode='wt', *, dialect='excel',
                 delimiter=None, quotechar=None, doublequote=None,
                 quoting=None, lineterminator=None, strict=True, encoding=None,
                 errors=None, handler=None, minimize=False, include=None,
                 rest_val=None):
        self.handler = handler
        self.minimize = minimize
        self.include = include
        self.rest_val = rest_val
        super().__init__(src, fields, mode, encoding, errors, dialect=dialect,
                         delimiter=delimiter, quotechar=quotechar,
                         doublequote=doublequote, quoting=quoting,
                         lineterminator=lineterminator, strict=strict)
        self._closed = False
