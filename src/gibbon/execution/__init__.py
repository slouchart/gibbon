from importlib import import_module


def get_async_executor(module_name):
    module = None
    try:
        module = import_module(module_name)

    except ModuleNotFoundError:
        module_name = 'asyncio'
        module = import_module(module_name)

    finally:
        assert module is not None

    try:
        local_adapter = import_module('.execution.models.' + module_name, 'gibbon')
    except BaseException as e:
        print(repr(e))
        return
    executor = local_adapter.get_async_executor()
    return executor


def get_sync_executor(module_name):
    module = None
    try:
        module = import_module(module_name)

    except ModuleNotFoundError:
        module_name = 'threading'
        module = import_module(module_name)

    finally:
        assert module is not None

    try:
        local_adapter = import_module('.execution.models.' + module_name, 'gibbon')
    except BaseException as e:
        print(repr(e))
        return
    executor = local_adapter.get_sync_executor()
    return executor



