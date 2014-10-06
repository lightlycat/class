package edu.clu.cs.inclass.utils;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.StringEscapeUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

public class MRDPUtils {

	private static DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	// This helper function parses the stackoverflow data into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");

			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];

				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}
	
	public static String getWikipediaURL(String text) {

		text = StringEscapeUtils.unescapeHtml(text.toLowerCase());
		int idx = text.indexOf("\"http://en.wikipedia.org");
		if (idx == -1) {
			return null;
		}
		int idx_end = text.indexOf('"', idx + 1);

		if (idx_end == -1) {
			return null;
		}

		int idx_hash = text.indexOf('#', idx + 1);

		if (idx_hash != -1 && idx_hash < idx_end) {
			return text.substring(idx + 1, idx_hash);
		} else {
			return text.substring(idx + 1, idx_end);
		}

	}
	public static String nestElements(String post, List<String> comments) {
		try {
			// Create the new document to build the XML
			DocumentBuilder bldr = dbf.newDocumentBuilder();
			Document doc = bldr.newDocument();

			// Copy parent node to document
			Element postEl = getXmlElementFromString(post);
			Element toAddPostEl = doc.createElement("post");

			// Copy the attributes of the original post element to the new
			// one
			copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

			// For each comment, copy it to the "post" node
			for (String commentXml : comments) {
				Element commentEl = getXmlElementFromString(commentXml);
				Element toAddCommentEl = doc.createElement("comments");

				// Copy the attributes of the original comment element to
				// the new one
				copyAttributesToElement(commentEl.getAttributes(),
						toAddCommentEl);

				// Add the copied comment to the post element
				toAddPostEl.appendChild(toAddCommentEl);
			}

			// Add the post element to the document
			doc.appendChild(toAddPostEl);

			// Transform the document into a String of XML and return
			return transformDocumentToString(doc);

		} catch (Exception e) {
			return null;
		}
	}

	private static Element getXmlElementFromString(String xml) {
		try {
			// Create a new document builder
			DocumentBuilder bldr = dbf.newDocumentBuilder();

			// Parse the XML string and return the first element
			return bldr.parse(new InputSource(new StringReader(xml)))
					.getDocumentElement();
		} catch (Exception e) {
			return null;
		}
	}

	private static void copyAttributesToElement(NamedNodeMap attributes,
			Element element) {

		// For each attribute, copy it to the element
		for (int i = 0; i < attributes.getLength(); ++i) {
			Attr toCopy = (Attr) attributes.item(i);
			element.setAttribute(toCopy.getName(), toCopy.getValue());
		}
	}

	private static String transformDocumentToString(Document doc) {
		try {
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			StringWriter writer = new StringWriter();
			transformer.transform(new DOMSource(doc), new StreamResult(
					writer));
			// Replace all new line characters with an empty string to have
			// one record per line.
			return writer.getBuffer().toString().replaceAll("\n|\r", "");
		} catch (Exception e) {
			return null;
		}
	}
}