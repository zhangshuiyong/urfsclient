interface resType {
  color: string
  content: string
}
export function getLabel(name: String): resType {
  switch (name) {
    case "中文分词":
      return { color: "pink", content: "中文分词" }

    case "图像分类":
      return { color: "red", content: "图像分类" }
        ;
    case "数据增强":
      return { color: "orange", content: "数据增强" }
        ;
    case "文本分类":
      return { color: "grey", content: "文本分类" }
        ;
    case "目标检测":
      return { color: "cyan", content: "目标检测" }
        ;
    case "目标跟踪":
      return { color: "blue", content: "目标跟踪" }

    case "语义分割":
      return { color: "yellow", content: "语义分割" }
    case "音频分类":
      return { color: "purple", content: "音频分类" }
    default:
      return { color: "green", content: "其他" }
  }
}
export function formatSize(s: string): string {
  let size=parseInt(s)
  let data = { number: 0, unit: "" };
  if (!size) return "0";
  var num = 1024.0; //byte
  if (size < num) {
    data.number = size;
    data.unit = "B";
    return data.number+data.unit;
  }
  if (size < Math.pow(num, 2)) {
    data.number = parseInt((size / num).toFixed(2));
    data.unit = "KB";
    return data.number+data.unit;
  }
  if (size < Math.pow(num, 3)) {
    data.number = parseInt((size / Math.pow(num, 2)).toFixed(2));
    data.unit = "MB";
    return data.number+data.unit;
  }
  if (size < Math.pow(num, 4)) {
    data.number = parseInt((size / Math.pow(num, 3)).toFixed(2));
    data.unit = "GB";
    return data.number+data.unit;
  } else {
    data.number = parseInt((size / Math.pow(num, 4)).toFixed(2));
    data.unit = "TB";
    return data.number+data.unit;
  }
}
