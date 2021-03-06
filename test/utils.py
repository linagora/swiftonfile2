from contextlib import ExitStack, contextmanager
import os


@contextmanager
def nested(*contexts):
    """
    Reimplementation of nested in python 3.
    """
    with ExitStack() as stack:
        for ctx in contexts:
            stack.enter_context(ctx)
        yield contexts


def check_equals_list_dict(list1, list2):
    assert [i for i in list1 if i not in list2] == []
    assert [i for i in list2 if i not in list1] == []


def touch_file(full_path):
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    open(full_path, "w").close()
