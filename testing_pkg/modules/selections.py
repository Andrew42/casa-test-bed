import awkward as ak
import numpy as np
from func_adl import ObjectStream

def get_lepton_query(source: ObjectStream) -> ObjectStream:
    """Performs event selection: require events with exactly four leptons.
    Also select all columns needed further downstream for processing &
    histogram filling.
    """
    return source.Where(lambda e: e.lep_n == 4).Select(
        lambda e: {
            "lep_pt": e.lep_pt,
            "lep_eta": e.lep_eta,
            "lep_phi": e.lep_phi,
            "lep_energy": e.lep_E,
            "lep_charge": e.lep_charge,
            "lep_typeid": e.lep_type,
            "mcWeight": e.mcWeight,
            "scaleFactor": e.scaleFactor_ELE
            * e.scaleFactor_MUON
            * e.scaleFactor_LepTRIGGER
            * e.scaleFactor_PILEUP,
            # scale factor systematic variation example
            "scaleFactorUP": e.scaleFactor_ELE
            * e.scaleFactor_MUON
            * e.scaleFactor_LepTRIGGER
            * e.scaleFactor_PILEUP
            * 1.1,
            "scaleFactorDOWN": e.scaleFactor_ELE
            * e.scaleFactor_MUON
            * e.scaleFactor_LepTRIGGER
            * e.scaleFactor_PILEUP
            * 0.9,
        }
    )

def lepton_filter(lep_charge, lep_type):
    """Filters leptons: sum of charges is required to be 0, and sum of lepton types 44/48/52.
    Electrons have type 11, muons have 13, so this means 4e/4mu/2e2mu.
    """
    sum_lep_charge = ak.sum(lep_charge, axis=1)
    sum_lep_type = ak.sum(lep_type, axis=1)
    good_lep_type = ak.any(
        [sum_lep_type == 44, sum_lep_type == 48, sum_lep_type == 52], axis=0
    )
    return ak.all([sum_lep_charge == 0, good_lep_type], axis=0)