from AWSClient import AWSClient
class AWSAnalysis(AWSClient):
    def __init__(self, resource: str, region: str, acc_id: str) -> None:
        super().__init__(resource, region, acc_id)