import mplhep
import hist

from processor import bin_edge_low, bin_edge_high, num_bins
from testing_pkg.modules import utils


def make_plots(all_histograms):
    # plot histograms with mplhep & hist
    mplhep.histplot(
        all_histograms["data"], histtype="errorbar", color="black", label="Data"
    )
    hist.Hist.plot1d(
        all_histograms["MC"][:, :, "nominal"],
        stack=True,
        histtype="fill",
        color=["purple", "red", "lightblue"],
    )

    # plot band for MC statistical uncertainties via utility function
    # (this uses matplotlib directly)
    utils.plot_errorband(bin_edge_low, bin_edge_high, num_bins, all_histograms)

    # we are using a small utility function to also save the figure in .png and .pdf
    # format, you can find the produced figure in the figures/ folder
    utils.save_figure("m4l_stat_uncertainty")