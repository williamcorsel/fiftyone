/**
 * Copyright 2017-2021, Voxel51, Inc.
 */

import { CHUNK_SIZE, LABELS, LABEL_LISTS } from "./constants";
import { deserialize } from "./numpy";
import { FrameChunk, FrameSample, Sample } from "./state";

/** GLOBALS */

const HIGH_WATER_MARK = 6;

let stream: FrameStream | null = null;
let streamId: string | null = null;

/** END GLOBALS */

const DESERIALIZE = {
  Detection: (label, buffers) => {
    if (typeof label.mask === "string") {
      label.mask = deserialize(label.mask);
      buffers.push(label.mask.buffer);
    }
  },
  Detections: (labels, buffers) => {
    labels.detections.forEach((label) => {
      if (typeof label.mask === "string") {
        label.mask = deserialize(label.mask);
        buffers.push(label.mask.buffer);
      }
    });
  },
  Segmentation: (label, buffers) => {
    if (typeof label.mask === "string") {
      label.mask = deserialize(label.mask);
      buffers.push(label.mask.buffer);
    }
  },
};

const mapId = (obj) => {
  obj.id = obj._id;
  delete obj._id;
  return obj;
};

const handleLabels = (sample: { [key: string]: any }): ArrayBuffer[] => {
  let buffers: ArrayBuffer[] = [];
  for (const field in sample) {
    const label = sample[field];
    if (!label) {
      continue;
    }
    if (label._cls in DESERIALIZE) {
      DESERIALIZE[label._cls](label, buffers);
    }

    if (label._cls in LABELS) {
      if (label._cls in LABEL_LISTS) {
        const list = label[LABEL_LISTS[label._cls]];
        if (Array.isArray(list)) {
          label[LABEL_LISTS[label._cls]] = list.map(mapId);
        }
      } else {
        mapId(label);
      }
    }
  }

  return buffers;
};

const handleSample = (
  sample: Sample & { frames?: [FrameSample] }
): ArrayBuffer[] => {
  let buffers = handleLabels(sample);

  if (sample.frames && sample.frames.length) {
    buffers = [
      ...buffers,
      ...sample.frames
        .map<ArrayBuffer[]>((frame) => handleLabels(frame))
        .flat(),
    ];
  }

  mapId(sample);

  return buffers;
};

interface ReaderMethod {
  method: string;
}

interface ProcessSample {
  uuid: string;
  sample: Sample & { frames: [FrameSample] };
}

type ProcessSampleMethod = ReaderMethod & ProcessSample;

const processSample = ({ sample, uuid }: ProcessSample) => {
  const buffers = handleSample(sample);

  postMessage(
    {
      method: "processSample",
      sample,
      uuid,
    },
    // @ts-ignore
    buffers
  );
};

interface FrameStream {
  chunkSize: number;
  frameNumber: number;
  sampleId: string;
  reader: ReadableStreamDefaultReader<FrameChunk>;
  cancel: () => void;
}

const createReader = ({
  chunkSize,
  frameCount,
  frameNumber,
  sampleId,
  source,
  url,
}: {
  chunkSize: number;
  frameCount: number;
  frameNumber: number;
  sampleId: string;
  source: boolean;
  url: string;
}): FrameStream => {
  let cancelled = false;

  const privateStream = new ReadableStream<FrameChunk>(
    {
      pull: (controller: ReadableStreamDefaultController) => {
        if (frameNumber >= frameCount || cancelled) {
          controller.close();
          return Promise.resolve();
        }

        return new Promise((resolve, reject) => {
          fetch(
            `${url}/frames?` +
              new URLSearchParams({
                frameNumber: frameNumber.toString(),
                numFrames: chunkSize.toString(),
                frameCount: frameCount.toString(),
                sampleId,
                source: source ? "1" : "0",
              })
          )
            .then((response: Response) => response.json())
            .then(({ frames, range }: FrameChunk) => {
              controller.enqueue({ frames, range });
              frameNumber = range[1] + 1;
              resolve();
            })
            .catch((error) => {
              reject(error);
            });
        });
      },
      cancel: () => {
        cancelled = true;
      },
    },
    new CountQueuingStrategy({ highWaterMark: HIGH_WATER_MARK })
  );
  return {
    sampleId,
    frameNumber,
    chunkSize,
    reader: privateStream.getReader(),
    cancel: () => (cancelled = true),
  };
};

const getSendChunk = (uuid: string) => ({
  value,
}: {
  done: boolean;
  value?: FrameChunk;
}) => {
  if (value) {
    let buffers: ArrayBuffer[] = [];

    value.frames.forEach((frame) => {
      buffers = [...buffers, ...handleLabels(frame)];
    });
    postMessage(
      {
        method: "frameChunk",
        frames: value.frames,
        range: value.range,
        uuid,
      },
      // @ts-ignore
      buffers
    );
  }
};

interface RequestFrameChunk {
  uuid: string;
}

type RequestFrameChunkMethod = ReaderMethod & RequestFrameChunk;

const requestFrameChunk = ({ uuid }: RequestFrameChunk) => {
  if (uuid === streamId) {
    stream && stream.reader.read().then(getSendChunk(uuid));
  }
};

interface SetStream {
  sampleId: string;
  source: boolean;
  frameNumber: number;
  frameCount: number;
  uuid: string;
  url: string;
}

type SetStreamMethod = ReaderMethod & SetStream;

const setStream = ({
  frameNumber,
  frameCount,
  sampleId,
  source,
  uuid,
  url,
}: SetStream) => {
  if (stream) {
    stream.cancel();
  }
  streamId = uuid;
  stream = createReader({
    chunkSize: CHUNK_SIZE,
    frameCount: frameCount,
    frameNumber: frameNumber,
    sampleId,
    source,
    url,
  });

  stream.reader.read().then(getSendChunk(uuid));
};

interface RequestSourceSample {
  sampleId: string;
  url: string;
  uuid: string;
}

const requestSourceSample = ({ sampleId, url, uuid }: RequestSourceSample) => {
  fetch(
    `${url}/sample?` +
      new URLSearchParams({
        sampleId,
      })
  )
    .then((response: Response) => response.json())
    .then(({ sample }: { sample: Sample }) => {
      const buffers = handleSample(sample);

      postMessage(
        {
          method: "sourceSample",
          sample,
          uuid,
        },
        // @ts-ignore
        buffers
      );
    });
};

type Method = SetStreamMethod | ProcessSampleMethod | RequestFrameChunkMethod;

onmessage = ({ data: { method, ...args } }: MessageEvent<Method>) => {
  switch (method) {
    case "processSample":
      processSample(args as ProcessSample);
      return;
    case "requestFrameChunk":
      requestFrameChunk(args as RequestFrameChunk);
      return;
    case "setStream":
      setStream(args as SetStream);
      return;
    case "requestSourceSample":
      requestSourceSample(args as RequestSourceSample);
      return;
    default:
      throw new Error("unknown method");
  }
};
