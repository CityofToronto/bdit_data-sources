---------------------------------------------------------------------------------------------------------------------------------------------------
--SIMILARITY BETWEEN ADDRESSES OF A COLLISION
---------------------------------------------------------------------------------------------------------------------------------------------------
--ALTER TABLE rbahreh.col_orig_abbr DROP COLUMN add1_add2_sim;
ALTER TABLE rbahreh.col_orig_nn ADD COLUMN add1_add2_trgm_dist numeric;

UPDATE rbahreh.col_orig_nn
SET add1_add2_trgm_dist = collision_add1<-> collision_add2;

--Absolute address
SELECT collision_add1, collision_add2, add1_add2_trgm_dist FROM rbahreh.col_orig_nn order by add1_add2_trgm_dist ASC;
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--SIMILARITY BETWEEN COLLISION ADDRESS1 AND CLOSEST CENTRELINE NAME
---------------------------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE rbahreh.col_orig_nn ADD COLUMN add1_lfname_nn_trgm_dist numeric;

UPDATE rbahreh.col_orig_nn
SET add1_lfname_nn_trgm_dist = collision_add1<-> lf_name;

SELECT Count(*) FROM rbahreh.col_orig_nn WHERE add1_lfname_nn_trgm_dist =0; --286167 (46%)
---------------------------------------------------------------------------------------------------------------------------------------------------
