import luigi
import os
import requests
import tarfile
import gzip
import shutil
import io
import pandas as pd


class GetDataset(luigi.Task):
    print("Загрузка датасета из источника")
    dataset = luigi.Parameter()
    folder = luigi.Parameter(default="data")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.folder, f"{self.dataset}_RAW.tar"))

    def run(self):

        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset}&format=file"
        os.makedirs(self.folder, exist_ok=True)
        # Загрузка датасета
        response = requests.get(url, stream=True)

        # Сохранение датасета в folder
        with open(self.output().path, "wb") as f:
            f.write(response.content)

class ExtractArchive(luigi.Task):
    print("Подготовка загруженных данных для дальнейшей обработки")
    dataset = luigi.Parameter()
    folder = luigi.Parameter(default="data")

    def requires(self):
        return GetDataset(self.dataset, self.folder)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.folder, self.dataset, "extracted"))

    def run(self):
        extracted_folder = self.output().path
        os.makedirs(extracted_folder, exist_ok=True)

        # Распаковка основного архива
        with tarfile.open(self.input().path, "r") as tar:
            tar.extractall(extracted_folder)

        # Распаковка каждого .gz файла в extracted_folder
        for file in os.listdir(extracted_folder):
            if file.endswith(".gz"):
                filepath = os.path.join(extracted_folder, file)

                with gzip.open(filepath, "rb") as f_in:

                    with open(filepath[:-3], "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Удаление оригинального .gz файла после распаковки
                os.remove(filepath)


class ProcessTables(luigi.Task):
    print("Обработка данных в удобное для нас представление")
    dataset = luigi.Parameter()
    folder = luigi.Parameter(default="data")

    def requires(self):
        return ExtractArchive(self.dataset, self.folder)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.folder, self.dataset))

    def run(self):
        work_folder = self.output().path
        os.makedirs(work_folder, exist_ok=True)
        extracted_folder = self.input().path

        for file in os.listdir(extracted_folder):
            if file.endswith(".txt"):
                input_path = os.path.join(extracted_folder, file)
                dfs = {}
                with open(input_path, "r") as f:
                    write_key = None
                    fio = io.StringIO()
                    for line in f.readlines():
                        if line.startswith('['):
                            if write_key:
                                fio.seek(0)
                                header = None if write_key == 'Heading' else 'infer'
                                dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                            fio = io.StringIO()
                            write_key = line.strip('[]\n')
                            continue
                        if write_key:
                            fio.write(line)
                    if write_key:
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t')

                for key, df in dfs.items():
                    output_file = os.path.join(work_folder, f"{key}.tsv")
                    df.to_csv(output_file, sep='\t', index=False)

                # Если среди таблиц есть "Probes", создается её урезанная версия
                if "Probes" in dfs:
                    reduced_df = dfs["Probes"].drop(columns=[
                        "Definition", "Ontology_Component", "Ontology_Process",
                        "Ontology_Function", "Synonyms", "Obsolete_Probe_Id", "Probe_Sequence"
                    ], errors="ignore")
                    reduced_file = os.path.join(work_folder, "Probes_reduced.tsv")
                    reduced_df.to_csv(reduced_file, sep='\t', index=False)

                # Удаление оригинального текстового файла после обработки
                os.remove(input_path)


class FinalTask(luigi.Task):
    print("Удаление промежуточных файлов")
    dataset = luigi.Parameter()
    folder = luigi.Parameter(default="data")

    def requires(self):
        return ProcessTables(self.dataset, self.folder)

    def run(self):
        # Удаление папки с промежуточными распакованными данными
        extracted_folder = os.path.join(self.folder, self.dataset, "extracted")
        if os.path.exists(extracted_folder):
            shutil.rmtree(extracted_folder)


if __name__ == "__main__":
    luigi.run()
