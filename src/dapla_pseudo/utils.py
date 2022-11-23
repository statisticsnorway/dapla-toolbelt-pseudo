"""Utilities"""
import typing as t


def find_multipart_obj(obj_name: str, multipart_files_tuple: t.Set[t.Any]) -> t.Any:
    """Find "multipart object" by name.

    The requests lib specifies multipart file arguments as file-tuples, such as
    ('filename', fileobj, 'content_type')
    This method searches a tuple of such file-tuples ((file-tuple1),...,(file-tupleN))
    It returns the fileobj for the first matching file-tuple with a specified filename.

    Example:
    Given the multipart_files_tuple:
    multipart_tuple = (('filename1', fileobj1, 'application/json'), ('filename2', fileobj2, 'application/json'))

    then
    find_multipart_obj("filename2", multipart_tuple) -> fileobj
    """
    try:
        matching_item = next(item[1] for item in multipart_files_tuple if item[0] == obj_name)
        return matching_item[1]
    except StopIteration:
        return None
