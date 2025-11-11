/*****************************************************************************
 *
 * Create Cast Statement
 *
 *****************************************************************************/
CreateCastStmt:
				CREATE_P OptTemp CAST '(' Typename AS Typename ')' WITH FUNCTION qualified_name create_cast_corecion_context
				{
					PGCreateCastStmt *n = makeNode(PGCreateCastStmt);
					n->relpersistence = $2;
					n->source = $5;
					n->target = $7;
					n->funcname = $11;
					n->context = $12;
					$$ = (PGNode *)n;
				}
				| CREATE_P OptTemp CAST '(' Typename AS Typename ')' WITH LAMBDA name SINGLE_COLON a_expr create_cast_corecion_context
				{
					PGCreateCastStmt *n = makeNode(PGCreateCastStmt);
					n->relpersistence = $2;
					n->source = $5;
					n->target = $7;
					n->lambda_name = $11;
					n->lambda_body = (PGExpr *) $13;
					n->is_lambda = true;
					n->context = $14;
					$$ = (PGNode *)n;
				}
		;

create_cast_corecion_context:
			AS IMPLICIT_P
			{
			     PGCreateCastContext c;
                 c.context = PG_COERCION_IMPLICIT;
                 c.cost = -1;
                 $$ = c;
			}
			| AS IMPLICIT_P COST ICONST
			{
				 PGCreateCastContext c;
				 c.context = PG_COERCION_IMPLICIT;
				 c.cost = $4;
				 $$ = c;
			}
			| /*EMPTY*/
			{
				 PGCreateCastContext c;
				 c.context = PG_COERCION_EXPLICIT;
				 c.cost = -1;
				 $$ = c;
			}
		;
