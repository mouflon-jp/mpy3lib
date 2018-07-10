#!/usr/bin/env python

import pandas as pd
from zipfile import ZipFile, ZIP_DEFLATED

try:
    # For Python 2
    from StringIO import StringIO as MemoryIO
except:
    # For Python 3
    from io import BytesIO as MemoryIO


def __options(opts=None, **default_opts):
    if opts is None:
        opts = {}
    options = default_opts
    options.update(opts)
    return options


def to_pickle_archive(dfs, file, archive_opts=None, pickle_opts=None):
    archive_opts = __options(archive_opts, compression=ZIP_DEFLATED, mode="a")
    pickle_opts = __options(pickle_opts, protocol=2)

    if not hasattr(dfs, "__iter__") or isinstance(dfs, pd.DataFrame):
        dfs = (dfs, )

    if isinstance(dfs, dict):
        dfs = dfs.items()
    else:
        dfs = enumerate(dfs)

    with ZipFile(file, **archive_opts) as zipf:
        for n, df in dfs:
            d = MemoryIO()
            df.to_pickle(d, **pickle_opts)
            zipf.writestr(str(n), d.getvalue())


def read_pickle_archive(file, names=None):
    if names is not None and not hasattr(names, "__iter__"):
        namelist = (names, )
    else:
        namelist = names
    
    with ZipFile(file, mode="r") as zipf:
        if namelist is None:
            namelist = zipf.namelist()

        ret = {}
        for name in namelist:
            with zipf.open(str(name)) as f:
                bdf = f.read()
            bdf = MemoryIO(bdf)
            df = pd.read_pickle(bdf)
            ret[name] = df

        if len(ret) == 1 and names is not None and not hasattr(names, "__iter__"):
            return ret.items()[0][1]
        else:
            return ret