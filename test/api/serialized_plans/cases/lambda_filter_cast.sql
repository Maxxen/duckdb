CREATE TABLE lfc (i INT, l INT[], v VARCHAR[]);
INSERT INTO lfc VALUES (1, [0, 1, 2, NULL], ['a', 'bb']), (2, [3, 0], ['ccc']), (3, NULL, NULL);
SELECT i, list_filter(l, lambda x: x), list_filter(v, lambda x, idx: length(x) - idx), list_transform(v, lambda x: list_filter(l, lambda y: y < length(x) + i)) FROM lfc ORDER BY i;
