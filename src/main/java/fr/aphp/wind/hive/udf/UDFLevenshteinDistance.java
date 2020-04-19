package fr.aphp.wind.hive.udf;

import info.debatty.java.stringsimilarity.CharacterSubstitutionInterface;
import info.debatty.java.stringsimilarity.WeightedLevenshtein;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.text.Normalizer;

@Description(name = "levenshtein_distance", value = "double _FUNC_(String c1, String c2) - get the Levenshtein distance between two strings")
public class UDFLevenshteinDistance extends GenericUDF {

    private StringDistance stringDistance;
    private PrimitiveObjectInspector inputsObj;
    private PrimitiveObjectInspector outputObj;


    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        stringDistance = new StringDistance();

        boolean areInputsAsString = true;
        //Check that we receive 2 arguments
        if (args.length != 2) {
            throw new UDFArgumentException(" Expecting 2 arguments ");
        }

        for (ObjectInspector arg : args) {
            areInputsAsString &= arg.getCategory() == ObjectInspector.Category.PRIMITIVE;
            if (areInputsAsString) {
                inputsObj = (PrimitiveObjectInspector) arg;
                areInputsAsString = inputsObj.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING;
            }
        }

        //Check that we receive 2 String arguments
        if (!areInputsAsString) {
            throw new UDFArgumentException(" Expecting all arguments to be String ");
        }

        outputObj = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        return outputObj;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {

        String c1, c2;

        if (args.length == 2 && args[0] != null && args[1] != null) {

            c1 = (String) inputsObj.getPrimitiveJavaObject(args[0].get());
            c2 = (String) inputsObj.getPrimitiveJavaObject(args[1].get());
        } else {
            return null;
        }
        return outputObj.getPrimitiveWritableObject(stringDistance.distance(c1, c2));
    }

    @Override
    public String getDisplayString(String[] children) {
        return "return_double = levenshtein_distance( '"+children[0]+"' , '"+children[1]+"' )";
    }

    /**
     * Calculates the Levenshtein distance between 2 strings
     */
//    public double evaluate(String c1, String c2){
//        return stringDistance.distance(c1,c2);
//    }

    private class StringDistance {
        WeightedLevenshtein distKernel;

        public StringDistance() {
            distKernel = new WeightedLevenshtein(new CharacterSubstitutionInterface() {
                public double cost(char c1, char c2) {

                    // The cost for substituting 't' and 'r' is considered
                    // smaller as these 2 are located next to each other
                    // on a keyboard
                    //TODO :
                    //penser aux tirets dans les mots

                    // majuscule en minuscule alors... pénalité...

                    if (c1 == String.valueOf(c2).toLowerCase().charAt(0)
                            || c2 == String.valueOf(c1).toLowerCase().charAt(0)) {
                        return 0.8;
                    }
                    // gestion accents...
                    if (stripAccents(String.valueOf(c1)).charAt(0) == stripAccents(String.valueOf(c2)).charAt(0)) {
                        return 0.1;
                    }
                    if ((c1 == 'o' || c1 == 'i' || c1 == 'l' || c1 == 'p' || c1 == 'ç' || c1 == 'à')
                            && (c2 == 'o' || c2 == 'i' || c2 == 'l' || c2 == 'p' || c2 == 'ç' || c2 == 'à')) {
                        return 0.5;
                    }
                    if ((c1 == 's' || c1 == 'z' || c1 == 'q' || c1 == 'd' || c1 == 'w' || c1 == 'x')
                            && (c2 == 's' || c2 == 'z' || c2 == 'q' || c2 == 'd' || c2 == 'w' || c2 == 'x')) {
                        return 0.5;
                    }
                    if ((c1 == 'd' || c1 == 'e' || c1 == 's' || c1 == 'f' || c1 == 'x' || c1 == 'c')
                            && (c2 == 'd' || c2 == 'e' || c2 == 's' || c2 == 'f' || c2 == 'x' || c2 == 'c')) {
                        return 0.5;
                    }
                    if ((c1 == 'f' || c1 == 'r' || c1 == 'd' || c1 == 'g' || c1 == 'c' || c1 == 'v')
                            && (c2 == 'f' || c2 == 'r' || c2 == 'd' || c2 == 'g' || c2 == 'c' || c2 == 'v')) {
                        return 0.5;
                    }
                    if ((c1 == 'g' || c1 == 't' || c1 == 'f' || c1 == 'h' || c1 == 'v' || c1 == 'b')
                            && (c2 == 'g' || c2 == 't' || c2 == 'f' || c2 == 'h' || c2 == 'v' || c2 == 'b')) {
                        return 0.5;
                    }
                    if ((c1 == 'h' || c1 == 'y' || c1 == 'g' || c1 == 'j' || c1 == 'b' || c1 == 'n')
                            && (c2 == 'h' || c2 == 'y' || c2 == 'g' || c2 == 'j' || c2 == 'b' || c2 == 'n')) {
                        return 0.5;
                    }
                    if ((c1 == 'j' || c1 == 'u' || c1 == 'h' || c1 == 'k' || c1 == 'n')
                            && (c2 == 'j' || c2 == 'u' || c2 == 'h' || c2 == 'k' || c2 == 'n')) {
                        return 0.5;
                    }
                    if ((c1 == 'x' || c1 == 'w' || c1 == 's' || c1 == 'd' || c1 == 'c')
                            && (c2 == 'x' || c2 == 'w' || c2 == 's' || c2 == 'd' || c2 == 'c')) {
                        return 0.5;
                    }
                    if ((c1 == 'c' || c1 == 'x' || c1 == 'd' || c1 == 'f' || c1 == 'v')
                            && (c2 == 'c' || c2 == 'x' || c2 == 'd' || c2 == 'f' || c2 == 'v')) {
                        return 0.5;
                    }
                    if ((c1 == 'v' || c1 == 'c' || c1 == 'f' || c1 == 'g' || c1 == 'b')
                            && (c2 == 'v' || c2 == 'c' || c2 == 'f' || c2 == 'g' || c2 == 'b')) {
                        return 0.5;
                    }
                    if ((c1 == 'b' || c1 == 'v' || c1 == 'g' || c1 == 'h' || c1 == 'n')
                            && (c2 == 'b' || c2 == 'v' || c2 == 'g' || c2 == 'h' || c2 == 'n')) {
                        return 0.5;
                    }
                    if ((c1 == 'z' || c1 == 'a' || c1 == 's' || c1 == 'e' || c1 == 'é')
                            && (c2 == 'z' || c2 == 'a' || c2 == 's' || c2 == 'e' || c2 == 'é')) {
                        return 0.5;
                    }
                    if ((c1 == 'y' || c1 == 't' || c1 == 'h' || c1 == 'u' || c1 == 'è')
                            && (c2 == 'y' || c2 == 't' || c2 == 'h' || c2 == 'u' || c2 == 'è')) {
                        return 0.5;
                    }
                    if ((c1 == 'u' || c1 == 'y' || c1 == 'j' || c1 == 'i' || c1 == 'è')
                            && (c2 == 'u' || c2 == 'y' || c2 == 'j' || c2 == 'i' || c2 == 'è')) {
                        return 0.5;
                    }
                    if ((c1 == 'i' || c1 == 'u' || c1 == 'k' || c1 == 'o' || c1 == 'ç')
                            && (c2 == 'i' || c2 == 'u' || c2 == 'k' || c2 == 'o' || c2 == 'ç')) {
                        return 0.5;
                    }
                    if ((c1 == 't' || c1 == 'r' || c1 == 'g' || c1 == 'y')
                            && (c2 == 't' || c2 == 'r' || c2 == 'g' || c2 == 'y')) {
                        return 0.5;
                    }
                    if ((c1 == 'a' || c1 == 'z' || c1 == 'q' || c1 == 'é')
                            && (c2 == 'a' || c2 == 'z' || c2 == 'q' || c2 == 'é')) {
                        return 0.5;
                    }
                    if ((c1 == 'e' || c1 == 'z' || c1 == 'r' || c1 == 'd')
                            && (c2 == 'e' || c2 == 'z' || c2 == 'r' || c2 == 'd')) {
                        return 0.5;
                    }
                    if ((c1 == 'r' || c1 == 'e' || c1 == 'f' || c1 == 't')
                            && (c2 == 'r' || c2 == 'e' || c2 == 'f' || c2 == 't')) {
                        return 0.5;
                    }
                    if ((c1 == 'p' || c1 == 'o' || c1 == 'm' || c1 == 'à')
                            && (c2 == 'p' || c2 == 'o' || c2 == 'm' || c2 == 'à')) {
                        return 0.5;
                    }
                    if ((c1 == 'q' || c1 == 'a' || c1 == 's' || c1 == 'w')
                            && (c2 == 'q' || c2 == 'a' || c2 == 's' || c2 == 'w')) {
                        return 0.5;
                    }
                    if ((c1 == 'k' || c1 == 'i' || c1 == 'j' || c1 == 'l')
                            && (c2 == 'k' || c2 == 'i' || c2 == 'j' || c2 == 'l')) {
                        return 0.5;
                    }
                    if ((c1 == 'l' || c1 == 'o' || c1 == 'k' || c1 == 'm')
                            && (c2 == 'l' || c2 == 'o' || c2 == 'k' || c2 == 'm')) {
                        return 0.5;
                    }
                    if ((c1 == 'm' || c1 == 'p' || c1 == 'l' || c1 == 'ù')
                            && (c2 == 'm' || c2 == 'p' || c2 == 'l' || c2 == 'ù')) {
                        return 0.5;
                    }
                    if ((c1 == 'w' || c1 == 'q' || c1 == 's' || c1 == 'x')
                            && (c2 == 'w' || c2 == 'q' || c2 == 's' || c2 == 'x')) {
                        return 0.5;
                    }
                    if ((c1 == 'n' || c1 == 'b' || c1 == 'h' || c1 == 'j')
                            && (c2 == 'n' || c2 == 'b' || c2 == 'h' || c2 == 'j')) {
                        return 0.5;
                    }
                    if ((c1 == 'é' || c1 == 'a' || c1 == 'z') && (c2 == 'é' || c2 == 'a' || c2 == 'z')) {
                        return 0.5;
                    }
                    if ((c1 == 'è' || c1 == 'y' || c1 == 'u') && (c2 == 'è' || c2 == 'y' || c2 == 'u')) {
                        return 0.5;
                    }
                    if ((c1 == 'ç' || c1 == 'i' || c1 == 'o') && (c2 == 'ç' || c2 == 'i' || c2 == 'o')) {
                        return 0.5;
                    }
                    if ((c1 == 'à' || c1 == 'o' || c1 == 'p') && (c2 == 'à' || c2 == 'o' || c2 == 'p')) {
                        return 0.5;
                    }


                    // For most cases, the cost of substituting 2 characters
                    // is 1.0
                    return 1.1;
                }
            });

        }

        private double distance(String c1, String c2) {
            return distKernel.distance(normaliseCase(c1), normaliseCase(c2));
        }

        private boolean isSimilar(String original, String candidate, boolean normAccent) {
            if (normAccent) {
                original = stripAccents(original);
                candidate = stripAccents(candidate);
            }
            if (original.length() != candidate.length()) {
                return false;
            }
            double seuil;
            double penality = 0;
            if (original.length() < 4) { // petit mot
                seuil = 0.1;
            } else if (original.length() < 6) { // mot long
                seuil = 0.5;
            } else { // petit mot
                seuil = original.length() / 3 * 0.5;
            }
            if (original.length() > 5) {// on substitue avant de calculer la distance
                ReorganizeString reor;
                reor = new ReorganizeString(candidate, original);
                reor.process();
                penality = reor.getSubstitutionNumber() / 1.5;
                candidate = reor.getReorganizedString();

            }
            return distance(candidate, original) + penality <= seuil;
        }

        private String stripAccents(String s) {
            s = Normalizer.normalize(s, Normalizer.Form.NFD);
            s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
            return s;
        }

        private String normaliseCase(String s) {
            char firstLetter = s.charAt(0);
            String nextLetters = s.substring(1, s.length()).toLowerCase();
            String allLetters = firstLetter + nextLetters;
            return allLetters;
        }

        private class ReorganizeString {
            private String goal;
            private String original;
            private int substitutionNumber = 0;

            public ReorganizeString(String original, String goal) {
                this.original = original;
                this.goal = goal;
            }

            public void process() {
                Integer step = 0;
                while (true) {
                    if (getWindOriginal(step).equals(getWindGoal(step))) {
                        //les morceaux sont égaux
                    } else if (substitute(getWindGoal(step)).equals(getWindOriginal(step))) {//une substitution rétablit...
                        original = transform(step, original, getWindGoal(step));
                        substitutionNumber++;
                    }
                    step++;//on avance dans le cadre
                    if (step + 1 >= original.length()) {
                        break;
                    }//la fin approche
                }
            }

            private String getWindOriginal(final Integer i) {
                return this.original.substring(i, i + 2);
            }

            private String getWindGoal(final Integer i) {
                return this.goal.substring(i, i + 2);
            }

            private String substitute(String db) {
                return String.format("%s%s", db.charAt(1), db.charAt(0));
            }

            private String transform(Integer step, String origin, String replacement) {
                return origin.substring(0, step) + replacement + origin.substring(step + 2, origin.length());
            }

            public Integer getSubstitutionNumber() {
                return this.substitutionNumber;
            }

            public String getReorganizedString() {
                return this.original;
            }
        }
    }


}