'''
    This file intended for documentation only, should disappear from first release (if any release occurs)

    Gibbon is an ETL framework in pure Python (3.7+)
    based around these ideas:

    1) the abstraction is a graph of tuple transformers, that graph is a DAG
    2) each transformer is either a source S, a target T, or an in-between transformation combining both T:S
    3) condition of validity is the existence of at least one path between any couple <S, T>
    4) mapping design and actual execution are decoupled
    5) the basic unit of processing is a tuple, namely a namedtuple with type enforcement (maybe not...)
    6) atomic datatypes are supported : int, float, str, bool, time, datetime.
    7) actual I/O handlers (databases, files, sockets, whatever) are selected at runtime
    8) actual execution mode (threaded, asynchronous, whatever) is selected at runtime
'''


def make_tuple(it):
    return (it[0], int(it[1]))


def adults(row):
    if row[1] >= 18:
        f = 'adult'
    else:
        f = 'non adult'
    return (*row, f)


def synopsis():

    from src import gibbon

    from samples import list_of_people_err, list_of_people
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # example of a simple mapping connecting a source and a destination through a filter
    workflow = gibbon.Workflow("Simple")

    workflow.add_source('SRC1')
    #workflow.add_source('SRC2')

    #workflow.add_complex_transformation('UNION', gibbon.Union, sources=('SRC1', 'SRC2'))

    workflow.add_transformation('IS_ADULT',
                               type=gibbon.Expression,
                               source='SRC1',
                               func=adults)

    workflow.add_target('DEST', source='IS_ADULT')

    workflow.validate(verbose=True)

    config = gibbon.Configuration()
    config.add_configuration('SRC1', source=gibbon.Sequence, data=list_of_people)
    #config.add_configuration('SRC2', source=gibbon.Sequence, data=list_of_people)
    config.add_configuration('DEST', target=gibbon.StdOut)

    executor = gibbon.get_async_executor()

    workflow.prepare(config)
    workflow.run(executor)
    workflow.reset(config)


if __name__ == '__main__':
    synopsis()
