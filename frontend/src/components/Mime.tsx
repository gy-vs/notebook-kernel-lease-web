import React from "react";

function asString(v: unknown): string {
  return typeof v === "string" ? v : JSON.stringify(v);
}

const IMAGE_PREFIXES = ["image/png", "image/jpeg", "image/gif", "image/webp"];

export const Mime: React.FC<{
  data: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}> = ({ data }) => {
  // 1) HTML
  if (typeof data["text/html"] === "string") {
    return (
      <div
        className="mime-html"
        dangerouslySetInnerHTML={{ __html: data["text/html"] }}
      />
    );
  }
  // 2) 图片
  const imgKey = IMAGE_PREFIXES.find((k) => typeof data[k] === "string");
  if (imgKey) {
    const mime = imgKey;
    return (
      <img
        className="mime-image"
        alt=""
        src={`data:${mime};base64,${(data[imgKey] as string).replace(/\n/g, "")}`}
      />
    );
  }
  // 3) SVG
  if (typeof data["image/svg+xml"] === "string") {
    return (
      <img
        className="mime-image"
        alt=""
        src={`data:image/svg+xml;utf8,${encodeURIComponent(data["image/svg+xml"])}`}
      />
    );
  }
  // 4) Markdown（极简：保留原文，交给 <pre>）—— 仍比执行结果丢失好
  if (typeof data["text/markdown"] === "string") {
    return <pre className="mime-text">{data["text/markdown"]}</pre>;
  }
  // 5) JSON
  if (data["application/json"] !== undefined) {
    return <pre className="mime-text">{JSON.stringify(data["application/json"], null, 2)}</pre>;
  }
  // 6) 纯文本
  if (data["text/plain"] !== undefined) {
    return <pre className="mime-text">{asString(data["text/plain"])}</pre>;
  }
  const keys = Object.keys(data);
  if (keys.length === 0) return null;
  return (
    <details className="mime-unknown">
      <summary>不支持的 MIME 类型：{keys.join(", ")}</summary>
      <pre>{asString(data[keys[0]])}</pre>
    </details>
  );
};
