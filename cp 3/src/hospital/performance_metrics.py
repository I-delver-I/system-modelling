"""
Collects overall performance metrics for the hospital model.
"""

from dataclasses import dataclass

from qnet.simulation_engine import ModelMetrics
from .patient_types import HospitalItem, SickType
from .utils import MeanMeter


@dataclass(eq=False)
class HospitalModelMetrics(ModelMetrics[HospitalItem]):
    """
    Specialized model metrics that also track mean times per SickType.
    """

    @property
    def mean_time_per_type(self) -> dict[SickType, float]:
        """
        Computes the average time in the system per patient type.
        """
        meters = {type_: MeanMeter() for type_ in SickType}
        for patient, time_ in self.time_per_item.items():
            meters[patient.sick_type].update(time_)
        return {type_: meter.mean for type_, meter in meters.items()}
