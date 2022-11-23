"""Module that implements a client abstraction that makes it easy to communicate with the Dapla Pseudo Service REST API."""

import mimetypes
import os
import typing as t

import requests
from dapla import AuthClient


class PseudoClient:
    """Client for interacting with the Dapla Pseudo Service REST API."""

    def __init__(
        self,
        pseudo_service_url: t.Optional[str] = None,
        auth_token: t.Optional[str] = None,
    ):
        """Use a default url for dapla-pseudo-service if not explicitly set."""
        self.pseudo_service_url = (
            "http://dapla-pseudo-service.dapla.svc.cluster.local" if pseudo_service_url is None else pseudo_service_url
        )
        self.static_auth_token = auth_token

    def __auth_token(self) -> str:
        return str(AuthClient.fetch_personal_token()) if self.static_auth_token is None else str(self.static_auth_token)

    def pseudonymize_file(self, request_json: str, file_path: str, stream: bool = False) -> requests.Response:
        """Pseudonymize a file (JSON or CSV - or a zip with potentially multiple such files) by uploading the file.

        Choose between streaming the result back, or storing it as a file in GCS (by providing a `targetUri`).

        Notice that you can specify the `targetContentType` if you want to convert to either of the supported file
        formats. E.g. your source could be a CSV file and the result could be a JSON file.

        Reduce transmission times by applying compression both to the source and target files.
        Specify `compression` if you want the result to be a zipped (and optionally) encrypted archive.

        Pseudonymization will be applied according to a list of "rules" that target the fields of the file being
        processed. Each rule defines a `pattern` (as a glob
        (https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob)) that identifies one or multiple
        fields, and a `func` that will be applied to the matching fields. Rules are processed in the order they are
        defined, and only the first matching rule will be applied (thus: rule ordering is important).

        Pseudo rules will most times refer to crypto keys. You can provide your own keys to use (via the `keysets`
        param) or use one of the predefined keys: `ssb-common-key-1` or `ssb-common-key-2`.

        See https://dapla-pseudo-service.staging-bip-app.ssb.no/api-docs/redoc#tag/Pseudo-operations/operation/pseudonymizeFile

        :param request_json: the request JSON to send to Dapla Pseudo Service
        :param file_path: path to a local file that should be pseudonymized
        :param stream: set to true if the results should be chunked into pieces, e.g. if you operate on large files.
        :return: pseudonymized data
        """
        return self._process_file("pseudonymize", request_json, file_path, stream)

    def depseudonymize_file(self, request_json: str, file_path: str, stream: bool = False) -> requests.Response:
        """Depseudonymize a file (JSON or CSV - or a zip with potentially multiple such files) by uploading the file.

        Notice that only certain whitelisted users can depseudonymize data.

        Choose between streaming the result back, or storing it as a file in GCS (by providing a `targetUri`).

        Notice that you can specify the `targetContentType` if you want to convert to either of the supported file
        formats. E.g. your source could be a CSV file and the result could be a JSON file.

        Reduce transmission times by applying compression both to the source and target files.
        Specify `compression` if you want the result to be a zipped (and optionally) encrypted archive.

        Depseudonymization will be applied according to a list of "rules" that target the fields of the file being
        processed. Each rule defines a `pattern` (as a
        glob (https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob)) that identifies one or multiple
        fields, and a `func` that will be applied to the matching fields. Rules are processed in the order they are
        defined, and only the first matching rule will be applied (thus: rule ordering is important).

        Pseudo rules will most times refer to crypto keys. You can provide your own keys to use (via the `keysets`
        param) or use one of the predefined keys: `ssb-common-key-1` or `ssb-common-key-2`.

        See https://dapla-pseudo-service.staging-bip-app.ssb.no/api-docs/redoc#tag/Pseudo-operations/operation/depseudonymizeFile

        :param request_json: the request JSON to send to Dapla Pseudo Service
        :param file_path: path to a local file that should be depseudonymized
        :param stream: set to true if the results should be chunked into pieces, e.g. if you operate on large files.
        :return: depseudonymized data
        """
        return self._process_file("depseudonymize", request_json, file_path, stream)

    def repseudonymize_file(self, request_json: str, file_path: str, stream: bool = False) -> requests.Response:
        """Repseudonymize a file (JSON or CSV - or a zip with potentially multiple such files) by uploading the file.

        Repseudonymization is done by first applying depseudonuymization and then pseudonymization to fields of the file.

        Choose between streaming the result back, or storing it as a file in GCS (by providing a `targetUri`).

        Notice that you can specify the `targetContentType` if you want to convert to either of the supported file
        formats. E.g. your source could be a CSV file and the result could be a JSON file.

        Reduce transmission times by applying compression both to the source and target files.
        Specify `compression` if you want the result to be a zipped (and optionally) encrypted archive.

        Repseudonymization will be applied according to a list of "rules" that target the fields of the file being
        processed. Each rule defines a `pattern` (as a
        glob (https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob)) that identifies one or multiple
        fields, and a `func` that will be applied to the matching fields. Rules are processed in the order they are
        defined, and only the first matching rule will be applied (thus: rule ordering is important). Two sets of rules
        are provided: one that defines how to depseudonymize and a second that defines how to pseudonymize. These sets
        of rules are linked to separate keysets.

        Pseudo rules will most times refer to crypto keys. You can provide your own keys to use (via the `keysets`
        param) or use one of the predefined keys: `ssb-common-key-1` or `ssb-common-key-2`.

        See https://dapla-pseudo-service.staging-bip-app.ssb.no/api-docs/redoc#tag/Pseudo-operations/operation/repseudonymizeFile

        :param request_json: the request JSON to send to Dapla Pseudo Service
        :param file_path: path to a local file that should be depseudonymized
        :param stream: set to true if the results should be chunked into pieces, e.g. if you operate on large files.
        :return: repseudonymized data
        """
        return self._process_file("repseudonymize", request_json, file_path, stream)

    def _process_file(
        self, operation: str, request_json: str, file_path: str, stream: bool = False
    ) -> requests.Response:
        file_name = os.path.basename(file_path).split("/")[-1]
        content_type = str(mimetypes.MimeTypes().guess_type(file_path)[0])
        auth_token = self.__auth_token()
        response = requests.post(
            url=f"{self.pseudo_service_url}/{operation}/file",
            headers={"Authorization": f"Bearer {auth_token}"},
            files={
                ("data", (file_name, open(file_path, "rb"), content_type)),
                ("request", (None, request_json, "application/json")),
            },
            stream=stream,
        )
        response.raise_for_status()
        return response

    def export_dataset(self, request_json: str) -> requests.Response:
        """Export a dataset in GCS to CSV or JSON, and optionally depseudonymize the data.

        The dataset will be archived in an encrypted zip file protected by a user provided password.

        It is possible to specify `columnSelectors`, that allows for partial export, e.g. only specific fields.
        This can be applied as a means to perform data minimization.

        Data is exported and stored to a specific, predefined GCS bucket. This is specified in the application
        configuration and cannot be overridden.

        See https://dapla-pseudo-service.staging-bip-app.ssb.no/api-docs/redoc#tag/Pseudo-operations/operation/export

        :param request_json: the request JSON to send to Dapla Pseudo Service
        :return: JSON response with a reference to the export "job"
        """
        auth_token = self.__auth_token()
        response = requests.post(
            url=f"{self.pseudo_service_url}/export",
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            },
            data=request_json,
        )
        response.raise_for_status()
        return response
