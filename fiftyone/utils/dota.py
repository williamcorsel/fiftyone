"""
Utilities for working with datasets in
`DOTA format <https://captain-whu.github.io/DOTA/dataset.html>`_.

| Copyright 2017-2022, Voxel51, Inc.
| `voxel51.com <https://voxel51.com/>`_
|
"""
import logging
import os
import csv

import eta.core.utils as etau
import eta.core.web as etaw

import fiftyone.core.labels as fol
import fiftyone.core.metadata as fom
import fiftyone.utils.data as foud


logger = logging.getLogger(__name__)


class DOTADetectionDatasetImporter(
    foud.LabeledImageDatasetImporter, foud.ImportPathsMixin
):
    """Importer for DOTA detection datasets stored on disk.

    See :ref:`this page <DOTADetectionDataset-import>` for format details.

    Args:
        dataset_dir (None): the dataset directory. If omitted, ``data_path``
            and/or ``labels_path`` must be provided
        data_path (None): an optional parameter that enables explicit control
            over the location of the media. Can be any of the following:

            -   a folder name like ``"data"`` or ``"data/"`` specifying a
                subfolder of ``dataset_dir`` where the media files reside
            -   an absolute directory path where the media files reside. In
                this case, the ``dataset_dir`` has no effect on the location of
                the data
            -   a filename like ``"data.json"`` specifying the filename of the
                JSON data manifest file in ``dataset_dir``
            -   an absolute filepath specifying the location of the JSON data
                manifest. In this case, ``dataset_dir`` has no effect on the
                location of the data
            -   a dict mapping filenames to absolute filepaths

            If None, this parameter will default to whichever of ``data/`` or
            ``data.json`` exists in the dataset directory
        labels_path (None): an optional parameter that enables explicit control
            over the location of the labels. Can be any of the following:

            -   a folder name like ``"labels"`` or ``"labels/"`` specifying the
                location of the labels in ``dataset_dir``
            -   an absolute folder path to the labels. In this case,
                ``dataset_dir`` has no effect on the location of the labels

            If None, the parameter will default to ``labels/``
        include_all_data (False): whether to generate samples for all images in
            the data directory (True) rather than only creating samples for
            images with label entries (False)
        extra_attrs (True): whether to load extra annotation attributes onto
            the imported labels. Supported values are:

            -   ``True``: load all extra attributes found
            -   ``False``: do not load extra attributes
            -   a name or list of names of specific attributes to load
        shuffle (False): whether to randomly shuffle the order in which the
            samples are imported
        seed (None): a random seed to use when shuffling
        max_samples (None): a maximum number of samples to import. By default,
            all samples are imported
    """
    def __init__(
        self,
        dataset_dir=None,
        data_path=None,
        labels_path=None,
        include_all_data=False,
        shuffle=False,
        seed=None,
        max_samples=None,
        **kwargs,
    ):
        if dataset_dir is None and data_path is None and labels_path is None:
            raise ValueError(
                "At least one of `dataset_dir`, `data_path`, and "
                "`labels_path` must be provided"
            )

        data_path = self._parse_data_path(
            dataset_dir=dataset_dir, data_path=data_path, default="images/",
        )

        labels_path = self._parse_labels_path(
            dataset_dir=dataset_dir,
            labels_path=labels_path,
            default="labelTxt/",
        )

        super().__init__(
            dataset_dir=dataset_dir,
            shuffle=shuffle,
            seed=seed,
            max_samples=max_samples,
        )
        # Your initialization here   
        self.data_path = data_path
        self.labels_path = labels_path
        self.include_all_data = include_all_data
        
        self._image_paths_map = None
        self._labels_paths_map = None
        self._uuids = None
        self._iter_uuids = None
        self._num_samples = None


    def __iter__(self):
        self._iter_uuids = iter(self._uuids)
        return self

    def __len__(self):
        return self._num_samples

    def __next__(self):
        uuid = next(self._iter_uuids)

        try:
            image_path = self._image_paths_map[uuid]
        except KeyError:
            raise ValueError("No image found for sample '%s'" % uuid)

        image_metadata = fom.ImageMetadata.build_for(image_path)

        labels_path = self._labels_paths_map.get(uuid, None)
        if labels_path:
            # Labeled image
            frame_size = (image_metadata.width, image_metadata.height)
            detections = load_dota_detection_annotations(
                labels_path,
                frame_size
            )
        else:
            # Unlabeled image
            detections = None

        return image_path, image_metadata, detections

    @property
    def has_dataset_info(self):
        return False

    @property
    def has_image_metadata(self):
        return True

    @property
    def label_cls(self):
        return fol.Polylines

    def setup(self):
        image_paths_map = self._load_data_map(
            self.data_path, ignore_exts=True, recursive=True
        )

        if self.labels_path is not None and os.path.isdir(self.labels_path):
            labels_paths_map = {
                os.path.splitext(p)[0]: os.path.join(self.labels_path, p)
                for p in etau.list_files(self.labels_path, recursive=True)
            }
        else:
            labels_paths_map = {}

        uuids = set(labels_paths_map.keys())

        if self.include_all_data:
            uuids.update(image_paths_map.keys())

        self._image_paths_map = image_paths_map
        self._labels_paths_map = labels_paths_map
        self._uuids = uuids
        self._num_samples = len(uuids)

    @staticmethod
    def _get_num_samples(dataset_dir):
        # Used only by dataset zoo
        return len(etau.list_files(os.path.join(dataset_dir, "data")))


def load_dota_detection_annotations(txt_path, frame_size):
    """Loads the DOTA detection annotations from the given TXT file.

    See :ref:`this page <DOTADetectionDataset-import>` for format details.

    Args:
        txt_path: the path to the annotations TXT file
        frame_size: the ``(width, height)`` of the image

    Returns:
        a :class:`fiftyone.core.detections.Polylines` instance
    """
    detections = []
    with open(txt_path) as f:
        reader = csv.reader(f, delimiter=" ")
        next(reader)
        next(reader)

        for row in reader:
            detections.append(
                _parse_dota_detection_row(row, frame_size)
            )

    return fol.Polylines(polylines=detections)


def _parse_dota_detection_row(row, frame_size):
    label = row[-2]
    data = iter(map(float, row[:8]))

    points = list(zip(data, data))
    points = [[(elem[0] / frame_size[0], elem[1] / frame_size[1]) for elem in points]]
    

    return fol.Polyline(
        label=label,
        points=points,
        closed=True
    )


_TRAIN_IMAGES_ZIP_URLS = (
    "1BlaGYNNEKGmT6OjZjsJ8HoUYrTTmFcO2",
    "1JBWCHdyZOd9ULX0ng5C9haAt3FMPXa3v",
    "1pEmwJtugIWhiwgBqOtplNUtTG2T454zn"
)
_TRAIN_LABELS_ZIP_URL = (
    "1I-faCP-DOxf6mxcjUTc8mYVPqUgSQxx6"
)

_VAL_IMAGES_ZIP_URLS = (
    "1uCCCFhFQOJLfjBpcL5MC0DHJ9lgOaXWP",
)
_VAL_LABELS_ZIP_URL = (
    "1FkCSOCy4ieNg1UZj1-Irfw6-Jgqa37cC"
)


def _get_dataset_info(dataset_dir, split):
    classes = [
        'plane', 'ship', 'storage-tank', 'baseball-diamond', 
        'tennis-court', 'basketball-court', 'ground-track-field',
        'harbor', 'bridge', 'large-vehicle', 'small-vehicle',
        'helicopter', 'roundabout', 'soccer-ball-field',
        'swimming-pool', 'container-crane'
    ]
    split_img_glob = os.path.join(
        dataset_dir, "images", "*.png"
    )
    num_samples = len(etau.get_glob_matches(split_img_glob))
    return num_samples, classes


def download_dota_dataset(
    dataset_dir, split, scratch_dir=None, cleanup=False
):
    """Downloads the DOTA object detection dataset from the web.

    The dataset will be organized on disk in as follows:

        dataset_dir/
            train/
                images/
                    P0000.png
                    P0001.png
                    ...
                labelTxt/
                    P0000.txt
                    P0001.txt
                    ...
            val/
                images/
                    P0000.png
                    P0001.png
                    ...
                labelTxt/
                    P0000.txt
                    P0001.txt
                    ...
            test/
                images/
                    P0000.png
                    P0001.png
                    ...

    Args:
        dataset_dir: the directory in which to construct the dataset
        split: the split to download. Supported values are
            ``("train", "validation", "test")``
        overwrite (True): whether to redownload the zips if they already exist
        cleanup (True): whether to delete the downloaded zips
    """
    etau.ensure_dir(dataset_dir)

    if scratch_dir is None:
        scratch_dir = os.path.join(dataset_dir, "scratch")
        etau.ensure_dir(scratch_dir)

    if split == "train":
        labels_url = _TRAIN_LABELS_ZIP_URL
        images_urls = _TRAIN_IMAGES_ZIP_URLS
    elif split == "val":
        labels_url = _VAL_LABELS_ZIP_URL
        images_urls = _VAL_IMAGES_ZIP_URLS
    # elif split == "test":
    #     labels_url = _TEST_LABELS_ZIP_URL
    #     images_urls = _VAL_IMAGES_ZIP_URLS

    if labels_url is not None:
        labels_zip_path = os.path.join(scratch_dir, "labelTxt.zip")
        if not os.path.exists(labels_zip_path):
            logger.info("Downloading labels to '%s'...", labels_zip_path)
            etaw.download_google_drive_file(labels_url, path=labels_zip_path)
        else:
            logger.info("Using existing labels '%s'", labels_zip_path)

    images_sub_zip_paths = []
    # images_zip_path = os.path.join(scratch_dir, "images")
    
    # print(images_zip_path)
        
    for i, images_sub_zip_url in enumerate(images_urls):
        images_sub_zip_path = os.path.join(scratch_dir, f"images{i}.zip")
        images_sub_zip_paths.append(images_sub_zip_path)
        if not os.path.exists(images_sub_zip_path):
            logger.info("Downloading images to '%s'...", images_sub_zip_path)
            print(images_sub_zip_url)
            etaw.download_google_drive_file(images_sub_zip_url, path=images_sub_zip_path)
        else:
            logger.info("Using existing images '%s'", images_sub_zip_path)


    logger.info("Extracting labels")
    etau.extract_zip(labels_zip_path, outdir=os.path.splitext(labels_zip_path)[0], delete_zip=cleanup)

    
    # for images_sub_zip_path in images_sub_zip_paths:
    #     logger.info(f"Extracting images {images_sub_zip_path}")
    #     sub_out_dir = os.path.splitext(images_sub_zip_path)[0]
    #     print(sub_out_dir)
        
    #     # etau.extract_zip(images_sub_zip_path, outdir=sub_out_dir, delete_zip=cleanup)
    #     print(dataset_dir)
    #     etau.move_dir(
    #         os.path.join(sub_out_dir, 'images'),
    #         os.path.join(dataset_dir, 'images'),
    #     )
    etau.move_dir(os.path.join(scratch_dir, "labelTxt"), os.path.join(dataset_dir, "labelTxt"))
    # etau.move_dir(
    #     os.path.join(scratch_dir, "labelTxt"),
    #     os.path.join(dataset_dir, "labelTxt", "data"),
    # )
 
    # etau.delete_dir(scratch_dir)

    num_samples, classes = _get_dataset_info(dataset_dir, split)

    return num_samples, classes
