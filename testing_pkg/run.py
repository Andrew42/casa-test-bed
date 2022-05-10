import asyncio
import os
import time

from coffea.processor import servicex
from func_adl_servicex import ServiceXSourceUpROOT

from testing_pkg.modules import utils
from testing_pkg.modules import selections
from processor import HZZAnalysis
from plotting import make_plots

async def produce_all_histograms(fs):
    """Runs the histogram production, processing input files with ServiceX and
    producing histograms with coffea.
    """
    # create the query
    ds = ServiceXSourceUpROOT("cernopendata://dummy", "mini", backend_name="uproot")
    ds.return_qastle = True
    lepton_query = selections.get_lepton_query(ds)

    # executor = servicex.LocalExecutor()
    # to run with the Dask executor on coffea-casa, use the following:
    # (please note https://github.com/CoffeaTeam/coffea/issues/611)
    if os.environ.get("LABEXTENTION_FACTORY_MODULE") == "coffea_casa":
        executor = servicex.DaskExecutor(client_addr="tls://localhost:8786")

    datasources = [
        utils.make_datasource(fs, ds_name, lepton_query)
        for ds_name in fs.keys()
    ]

    # create the analysis processor
    analysis_processor = HZZAnalysis()

    async def run_updates_stream(accumulator_stream, name):
        """Run to get the last item in the stream"""
        coffea_info = None
        try:
            async for coffea_info in accumulator_stream:
                pass
        except Exception as e:
            raise Exception(f"Failure while processing {name}") from e
        return coffea_info

    all_histogram_dicts = await asyncio.gather(
        *[
            run_updates_stream(
                executor.execute(analysis_processor, source),
                source.metadata["dataset_category"],
            )
            for source in datasources
        ]
    )
    full_data_histogram = sum([h["data"] for h in all_histogram_dicts])
    full_mc_histogram = sum([h["MC"] for h in all_histogram_dicts])
    
    return {"data": full_data_histogram, "MC": full_mc_histogram}



utils.clean_up()  # delete output from previous runs of notebook (optional)
# utils.set_logging()  # configure logging output

prefix = (
    "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/"
)

# labels for combinations of datasets
z_ttbar = r"Background $Z,t\bar{t}$"
zzstar = r"Background $ZZ^{\star}$"
signal = r"Signal ($m_H$ = 125 GeV)"

fileset = {
    "Data": [
        prefix + "Data/data_A.4lep.root",
        prefix + "Data/data_B.4lep.root",
        prefix + "Data/data_C.4lep.root",
        prefix + "Data/data_D.4lep.root",
    ],
    z_ttbar: [
        prefix + "MC/mc_361106.Zee.4lep.root",
        prefix + "MC/mc_361107.Zmumu.4lep.root",
        prefix + "MC/mc_410000.ttbar_lep.4lep.root",
    ],
    zzstar: [prefix + "MC/mc_363490.llll.4lep.root"],
    signal: [
        prefix + "MC/mc_345060.ggH125_ZZ4lep.4lep.root",
        prefix + "MC/mc_344235.VBFH125_ZZ4lep.4lep.root",
        prefix + "MC/mc_341964.WH125_ZZ4lep.4lep.root",
        prefix + "MC/mc_341947.ZH125_ZZ4lep.4lep.root",
    ],
}

t0 = time.time()

# in a notebook:
# all_histograms = await produce_all_histograms()

# as a script:
async def produce_all_the_histograms()fileset:
   return await produce_all_histograms(fileset)

all_histograms = asyncio.run(produce_all_the_histograms(fileset))

print(f"execution took {time.time() - t0:.2f} seconds")

make_plots(all_histograms)