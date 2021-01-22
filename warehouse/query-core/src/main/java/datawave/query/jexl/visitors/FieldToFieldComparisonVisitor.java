package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.*;

public class FieldToFieldComparisonVisitor extends RebuildingVisitor {
    /**
     * force evaluation for field to field comparison
     *
     * @param root
     * @return
     */
    public static ASTJexlScript forceEvaluationOnly(JexlNode root) {
        FieldToFieldComparisonVisitor vis = new FieldToFieldComparisonVisitor();
        return (ASTJexlScript) root.jjtAccept(vis, null);
    }
    
    /**
     * detect identifier on both sides of nodes and wrap it with evaluation-only reference
     *
     * @param node
     * @return
     */
    protected JexlNode evaluationOnlyForFieldToFieldComparison(JexlNode node) {
        int identifierNodes = 0;
        
        // check both sides of nodes and count the nodes with identifier(s)
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            if (JexlASTHelper.getIdentifiers(node.jjtGetChild(i)).size() > 0) {
                identifierNodes++;
            }
        }
        
        if (identifierNodes > 1) {
            return ASTEvaluationOnly.create(node);
        }
        return node;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return evaluationOnlyForFieldToFieldComparison(node);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return evaluationOnlyForFieldToFieldComparison(node);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return evaluationOnlyForFieldToFieldComparison(node);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return evaluationOnlyForFieldToFieldComparison(node);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return evaluationOnlyForFieldToFieldComparison(node);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return evaluationOnlyForFieldToFieldComparison(node);
    }
}
