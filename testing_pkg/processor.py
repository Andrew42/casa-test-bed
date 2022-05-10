import re

from coffea.processor import servicex
import hist

from testing_pkg.modules.selections import lepton_filter
from testing_pkg.modules.xsec_tools import get_xsec_weight

# information for histograms
bin_edge_low = 80  # 80 GeV
bin_edge_high = 250  # 250 GeV
num_bins = 34

class HZZAnalysis(servicex.Analysis):
    """The coffea processor used in this analysis."""

    def process(self, events):

        # type of dataset being processed, provided via metadata (comes originally from fileset)
        dataset_category = events.metadata["dataset_category"]

        # get lepton information from events
        leptons = events.lep

        # apply a cut to events, based on lepton charge and lepton type
        lepton_mask = lepton_filter(leptons.charge, leptons.typeid)
        events = events[lepton_mask]
        leptons = events.lep

        # calculate the 4-lepton invariant mass for each remaining event
        # this could also be an expensive calculation using external tools
        mllll = (
            leptons[:, 0] + leptons[:, 1] + leptons[:, 2] + leptons[:, 3]
        ).mass / 1000

        # creat histogram holding outputs, for data just binned in m4l
        mllllhist_data = hist.Hist.new.Reg(
            num_bins,
            bin_edge_low,
            bin_edge_high,
            name="mllll",
            label="$\mathrm{m_{4l}}$ [GeV]",
        ).Weight()  # using weighted storage here for plotting later, but not needed

        # three histogram axes for MC: m4l, category, and variation (nominal and
        # systematic variations)
        mllllhist_MC = (
            hist.Hist.new.Reg(
                num_bins,
                bin_edge_low,
                bin_edge_high,
                name="mllll",
                label="$\mathrm{m_{4l}}$ [GeV]",
            )
            .StrCat([k for k in fileset.keys() if k != "Data"], name="dataset")
            .StrCat(
                ["nominal", "scaleFactorUP", "scaleFactorDOWN", "m4lUP", "m4lDOWN"],
                name="variation",
            )
            .Weight()
        )

        if dataset_category == "Data":
            # create and fill a histogram for m4l
            mllllhist_data.fill(mllll=mllll)

        else:
            # extract the sample name from the filename to calculate x-sec weight
            sample = re.findall(r"mc_\d+\.(.+)\.4lep", events.metadata["filename"])[0]
            basic_weight = get_xsec_weight(sample) * events.mcWeight
            totalWeights = basic_weight * events.scaleFactor

            # calculate systematic variations for weight
            totalWeightsUp = basic_weight * events.scaleFactorUP
            totalWeightsDown = basic_weight * events.scaleFactorDOWN

            # create and fill weighted histograms for m4l: nominal and variations
            mllllhist_MC.fill(
                mllll=mllll,
                dataset=dataset_category,
                variation="nominal",
                weight=totalWeights,
            )

            # scale factor variations
            mllllhist_MC.fill(
                mllll=mllll,
                dataset=dataset_category,
                variation="scaleFactorUP",
                weight=totalWeightsUp,
            )
            mllllhist_MC.fill(
                mllll=mllll,
                dataset=dataset_category,
                variation="scaleFactorDOWN",
                weight=totalWeightsDown,
            )

            # variation in 4-lepton invariant mass
            mllllhist_MC.fill(
                mllll=mllll * 1.01,
                dataset=dataset_category,
                variation="m4lUP",
                weight=totalWeights,
            )
            mllllhist_MC.fill(
                mllll=mllll * 0.99,
                dataset=dataset_category,
                variation="m4lDOWN",
                weight=totalWeights,
            )

        return {"data": mllllhist_data, "MC": mllllhist_MC}