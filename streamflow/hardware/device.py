from streamflow.core.hardware import Device


class NVIDIAGPUDevice(Device):

    def __init__(self, compute_capability: float, cuda_version: float) -> None:
        super().__init__(
            capabilities={"compute": compute_capability, "cuda": cuda_version},
            vendor="NVIDIA",
        )

    def check_compute(self, value: float) -> bool:
        return value < self.capabilities["compute"]

    def check_cuda(self, value: float) -> bool:
        return value < self.capabilities["cuda"]
