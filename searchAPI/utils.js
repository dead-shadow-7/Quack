function tokenize(text) {
  text = text.toLowerCase();

  return text
    .replace(/[^a-z0-9 ]/g, "")
    .split(" ")
    .filter((word) => word.length > 2);
}

module.exports = { tokenize };
