/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/

package ai.vital.predict.categories;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.Edge_hasChildCategory;
import ai.vital.predict.categories.HierarchicalCategories.TaxonomyNode;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalutilcommands.AbstractUtil;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;

public class TaxonomyOWLBridge extends AbstractUtil {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		if(args.length != 4) {
			o("Usage: <input_taxonomy_txt> <input_ontology> <output_individuals_owl> <output_merged_ontology>");
			return;
		}
		
		//creates a list of individuals that can be then imported into ontology
		File taxF = new File(args[0]);
		o("Input taxonomy file: " + taxF.getAbsolutePath());

		File inputOntology = new File(args[1]);
		o("Input ontology: " + inputOntology.getAbsolutePath());
		
		File outputF = new File(args[2]);
		o("Output individuals: " + outputF.getAbsolutePath());
		
		File outputMergedF = new File(args[3]);
		o("Output merged ontology: " + outputMergedF.getAbsolutePath());
		
		HierarchicalCategories mapping = new HierarchicalCategories(taxF);
		
		TaxonomyNode taxonomyRoot = mapping.getRootNode();

		Model model = ModelFactory.createDefaultModel();

		
		writeNode(model, taxonomyRoot);
		
		BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(outputF));
		model.write(os);
		os.close();
		
		
		model.read(new FileInputStream(inputOntology), null);
		
		
		os = new BufferedOutputStream(new FileOutputStream(outputMergedF));
		model.write(os);
		os.close();
		
		o("Done");

	}

	public static void writeNode(Model model, TaxonomyNode node) {
	
		VITAL_Category category = new VITAL_Category();
		category.setURI(node.getURI());
		category.setProperty("name", node.getLabel());
		
		TaxonomyNode parent = node.getParent();

		writeGraphObject(model, category);
		
		if(parent != null) {
			
			Edge_hasChildCategory edge = new Edge_hasChildCategory();
			edge.setURI("http://uri.vital.ai/Edge_hasChildCategory/" + localURI(parent.getURI()) + "__" + localURI(node.getURI()));
			edge.setSourceURI(parent.getURI());
			edge.setDestinationURI(node.getURI());
			
			writeGraphObject(model, edge);
			
		}
		
		for(TaxonomyNode ch : node.getChildren()) {
			writeNode(model, ch);
		}
		
	}
	
	private static void writeGraphObject(Model model, GraphObject cat) {
		Resource r = cat.toRDF(model);
		r.addProperty(RDF.type, ResourceFactory.createResource( OWL.NS + "NamedIndividual" ) );
	}

	private static String localURI(String uri) {
		int lastIndexOf = uri.lastIndexOf('/');
		if(lastIndexOf >0 && lastIndexOf < uri.length() - 1 ) {
			return uri.substring(lastIndexOf + 1);
		}
		return uri;
	}

}
