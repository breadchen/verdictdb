package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.condition.CondModifier;
import edu.umich.verdict.relation.condition.OrCond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.OrderByExpr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.relation.expr.SubqueryExpr;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

public abstract class ApproxRelation extends Relation {

	public ApproxRelation(VerdictContext vc) {
		super(vc);
		approximate = true;
	}
	
	public String errColSuffix() {
		return "_err";
	}
	
	public String sourceTableName() {
		if (this instanceof ApproxSingleRelation) {
			ApproxSingleRelation r = (ApproxSingleRelation) this;
			if (r.getAliasName() != null) {
				return r.getAliasName();
			} else {
				return r.getSampleName().tableName;
			}
		} else {
			return this.getAliasName();
		}
	}
	
	/*
	 * Aggregations
	 */
	
	public ApproxGroupedRelation groupby(String group) {
		String[] tokens = group.split(",");
		return groupby(Arrays.asList(tokens));
	}
	
	public ApproxGroupedRelation groupby(List<String> group_list) {
		List<ColNameExpr> groups = new ArrayList<ColNameExpr>();
		for (String t : group_list) {
			groups.add(ColNameExpr.from(t));
		}
		return new ApproxGroupedRelation(vc, this, groups);
	}
	
	/*
	 * Approx
	 */
	
	public ApproxAggregatedRelation agg(Object... elems) {
		return agg(Arrays.asList(elems));
	}
	
	public ApproxAggregatedRelation agg(List<Object> elems) {
		List<SelectElem> se = new ArrayList<SelectElem>();
		for (Object e : elems) {
			se.add(SelectElem.from(e.toString()));
		}
		return new ApproxAggregatedRelation(vc, this, se);
	}

	@Override
	public ApproxAggregatedRelation count() throws VerdictException {
		return agg(FuncExpr.count());
	}

	@Override
	public ApproxAggregatedRelation sum(String expr) throws VerdictException {
		return agg(FuncExpr.sum(Expr.from(expr)));
	}

	@Override
	public ApproxAggregatedRelation avg(String expr) throws VerdictException {
		return agg(FuncExpr.avg(Expr.from(expr)));
	}

	@Override
	public ApproxAggregatedRelation countDistinct(String expr) throws VerdictException {
		return agg(FuncExpr.countDistinct(Expr.from(expr)));
	}
	
	/**
	 * Properly scale all aggregation functions so that the final answers are correct.
	 * For ApproxAggregatedRelation: returns a AggregatedRelation instance whose result is approximately correct.
	 * For ApproxSingleRelation, ApproxJoinedRelation, and ApproxFilteredRelaation: returns
	 * a select statement from sample tables. The rewritten sql doesn't have much meaning if not used by ApproxAggregatedRelation. 
	 * @return
	 */
	public ExactRelation rewrite() {
		if (vc.getConf().get("verdict.error_bound_method").equals("nobound")) {
			return rewriteForPointEstimate();
		} else if (vc.getConf().get("verdict.error_bound_method").equals("subsampling")) {
			return rewriteWithSubsampledErrorBounds();
		} else if (vc.getConf().get("verdict.error_bound_method").equals("bootstrapping")) {
			return rewriteWithBootstrappedErrorBounds();
		} else {
			VerdictLogger.error(this, "Unsupported error bound computation method: " + vc.getConf().get("verdict.error_bound_method"));
			return null;
		}
	}
	
	public abstract ExactRelation rewriteForPointEstimate();
	
	
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		VerdictLogger.error(this, String.format("Calling a method, %s, on unappropriate class", "rewriteWithSubsampledErrorBounds()"));
		return null;
	}
	
	/**
	 * Internal method for {@link ApproxRelation#rewriteWithSubsampledErrorBounds()}.
	 * @return
	 */
	protected abstract ExactRelation rewriteWithPartition();
	
//	protected String partitionColumnName() {
//		return vc.getDbms().partitionColumnName();
//	}
	
	// returns effective partition column name for a possibly joined table.
//	protected abstract ColNameExpr partitionColumn();
	
	public ExactRelation rewriteWithBootstrappedErrorBounds() { return null; }
	
	/**
	 * Computes an appropriate sampling probability for a particular aggregate function.
	 * For uniform random sample, returns the ratio between the sample table and the original table.
	 * For universe sample,
	 *  if the aggregate function is COUNT, AVG, SUM, returns the ratio between the sample table and the original table.
	 *  if the aggregate function is COUNT-DISTINCT, returns the sampling probability.
	 * For stratified sample, this method returns the sampling probability only for the joined tables.
	 * 
	 * Verdict sample rules.
	 * 
	 * For COUNT, AVG, and SUM, uniform random samples, universe samples, stratified samples, or no samples can be used.
	 * For COUNT-DISTINCT, universe sample, stratified samples, or no samples can be used. For stratified samples, the
	 * distinct number of groups is assumed to be limited.
	 * 
	 * Verdict join rules.
	 * 
	 * (uniform, uniform)       -> uniform
	 * (uniform, stratified)    -> stratified
	 * (uniform, universe)		-> uniform
	 * (uniform, no sample)     -> uniform
	 * (stratified, stratified) -> stratified
	 * (stratified, universe)   -> no allowed
	 * (stratified, no sample)  -> stratified
	 * (universe, universe)     -> universe   (only when the columns on which samples are built coincide)
	 * (universe, no sample)    -> universe
	 * 
	 * @param f
	 * @return
	 */
	protected abstract List<Expr> samplingProbabilityExprsFor(FuncExpr f);
	
	/**
	 * Returns an effective sample type of this relation.
	 * @return One of "uniform", "universe", "stratified", "nosample".
	 */
	protected abstract String sampleType();
	
//	protected abstract List<ColNameExpr> accumulateSamplingProbColumns();
	
	/**
	 * Returns a set of columns on which a sample is created. Only meaningful for stratified and universe samples.
	 * @return
	 */
	protected abstract List<String> sampleColumns();
	
	/**
	 * Pairs of original table name and a sample table name. This function does not inspect subqueries.
	 * @return
	 */
	protected abstract Map<String, String> tableSubstitution();
	
	
	/*
	 * order by and limit
	 */
	
	public ApproxRelation orderby(String orderby) {
		String[] tokens = orderby.split(",");
		List<OrderByExpr> o = new ArrayList<OrderByExpr>();
		for (String t : tokens) {
			o.add(OrderByExpr.from(t));
		}
		return new ApproxOrderedRelation(vc, this, o);
	}
	
	public ApproxRelation limit(long limit) {
		return new ApproxLimitedRelation(vc, this, limit);
	}
	
	/*
	 * sql
	 */

	@Override
	public String toSql() {
		ExactRelation r = rewrite();
		return r.toSql();
	}
	
	/*
	 * Helpers
	 */
	
	protected Expr exprWithTableNamesSubstituted(Expr expr, final Map<String, String> sub) {
		ExprModifier v = new ExprModifier() {
			public Expr call(Expr expr) {
				if (expr instanceof ColNameExpr) {
					ColNameExpr e = (ColNameExpr) expr;
					return new ColNameExpr(e.getCol(), sub.get(e.getTab()), e.getSchema());
				} else if (expr instanceof FuncExpr) {
					FuncExpr e = (FuncExpr) expr;
					return new FuncExpr(e.getFuncName(), visit(e.getUnaryExpr()));
				} else {
					return expr;
				}
			}
		};
		return v.visit(expr);
	}

}