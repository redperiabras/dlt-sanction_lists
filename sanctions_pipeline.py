import dlt

from source.interpol import src_interpol
from source.unsc import src_unsc

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="sanctions",
        destination='duckdb',
        dataset_name="interpol",
        progress=dlt.progress.tqdm(colour="yellow"),
    )

    load_info = pipeline.run(src_interpol())
    
    # pipeline = dlt.pipeline(
    #     pipeline_name="sanctions",
    #     destination='duckdb',
    #     dataset_name="unsc",
    #     progress=dlt.progress.tqdm(colour="yellow"),
    # )
    
    # load_info = pipeline.run(src_unsc())

    print(load_info)
