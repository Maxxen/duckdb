CREATE TABLE t1 (g INT, d DECIMAL(10,3), w DECIMAL(50,3));
INSERT INTO t1 VALUES (1, 1.5, 1.5), (1, 2.5, 2.5), (1, 3.5, 3.5), (2, 10.5, 10.5);
SELECT g, finalize(s1), finalize(s2), finalize(s3), finalize(s4), finalize(s5), finalize(s6) FROM (SELECT g, approx_quantile(d, 0.5) EXPORT_STATE s1, approx_quantile(d, [0.25, 0.75]) EXPORT_STATE s2, quantile_disc(d, 0.5) EXPORT_STATE s3, approx_quantile(w, 0.5) EXPORT_STATE s4, quantile_disc(d, [0.25, 0.75]) EXPORT_STATE s5, approx_quantile(w, [0.5]) EXPORT_STATE s6 FROM t1 GROUP BY g) ORDER BY g;
