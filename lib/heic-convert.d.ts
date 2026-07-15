declare module "heic-convert" {
  type HeicOutputFormat = "JPEG" | "PNG";

  interface HeicConvertOptions {
    readonly buffer: Buffer | Uint8Array | ArrayBuffer;
    readonly format: HeicOutputFormat;
    readonly quality?: number | undefined;
  }

  function convert(options: HeicConvertOptions): Promise<Buffer>;

  namespace convert {
    function all(options: HeicConvertOptions): Promise<readonly Buffer[]>;
  }

  export = convert;
}
