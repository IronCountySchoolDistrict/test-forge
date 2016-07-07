import { logErrors } from '../../logger';

/**
 * create test date field based on year test was taken
 * @param {object} studentTestConceptResult row of crt data with student test concepts results
 * @param {string} studentTestConceptResult.school_year eg. "2001"
 * @return {object}
 */
export function createTestDate(studentTestConceptResult) {
  const testDate = new Date(`05/01/${studentTestConceptResult.school_year}`);
  studentTestConceptResult.test_date = `${testDate.getMonth() + 1}/${testDate.getDay() + 1}/${testDate.getFullYear()}`
  return studentTestConceptResult;
}

export function correctTestProgramDesc(test_program_desc) {
  test_program_desc = test_program_desc === 'Earth Sytems Science' ? 'Earth Systems Science' : test_program_desc;
  test_program_desc = test_program_desc === 'Algebra 1' ? 'Algebra I' : test_program_desc;
  test_program_desc = test_program_desc === 'Algebra 2' ? 'Algebra II' : test_program_desc;
  return test_program_desc;
}

export function correctConceptDesc(conceptDesc) {
  // remove roman numerals
  conceptDesc = conceptDesc.replace(/^(X{0,3}\s+|IX\s+|IV\s+|V?I{0,3}\s*)/g, '');

  // remove all dots
  conceptDesc = conceptDesc.replace(/(\.)+$/g, '');

  // remove all numbers at the beginning of concept_desc
  conceptDesc = conceptDesc.replace(/^([0-9]+)\s+/g, '');

  // remove all asterisks
  conceptDesc = conceptDesc.replace(/(\*)+/ig, '');

  // spelling corrections
  conceptDesc = conceptDesc.replace(/(Comprhnsn)/ig, 'Comprehension');
  conceptDesc = conceptDesc.replace(/(\band\b)/ig, '&');
  conceptDesc = conceptDesc.replace(/(intgrted)/ig, 'integrated');
  conceptDesc = conceptDesc.replace(/(Cycle As It Orbits Earth)/ig, 'Cycle As It Orbits The Earth');
  conceptDesc = conceptDesc.replace(/(\bAmts\b)/ig, 'Amounts');
  conceptDesc = conceptDesc.replace(/(Forms, But)/ig, 'Forms &');
  conceptDesc = conceptDesc.replace(/(\bSpatial, )/ig, 'Spacial ');
  conceptDesc = conceptDesc.replace(/(\bSituatio\b)/ig, 'Situations');
  conceptDesc = conceptDesc.replace(/(Hydrosphere, Affects)/ig, 'Hydrosphere Affects');
  conceptDesc = conceptDesc.replace(/(Environment & Humans Impact On)/ig, 'Environment & Human Impact On');
  conceptDesc = conceptDesc.replace(/(Uplife,)/ig, 'Uplift,');
  conceptDesc = conceptDesc.replace(/(Concepts From Statistics & Apply)/ig, 'Concepts From Probability & Statistics & Apply');
  conceptDesc = conceptDesc.replace(/(Reasonable Conlusions)/ig, 'Reasonable Conclusions');
  conceptDesc = conceptDesc.replace(/\b(\/)/g, ' /');
  conceptDesc = conceptDesc.replace(/(\/)\b/g, '/ ');
  conceptDesc = conceptDesc.replace(/(Force, Mass, &)/ig, 'Force, Mass &');
  conceptDesc = conceptDesc.replace(/(Acclerations)/ig, 'Acceleration');
  conceptDesc = conceptDesc.replace(/(Accelerations)/ig, 'Acceleration');
  conceptDesc = conceptDesc.replace(/(& Application Of Waves)/ig, '& Applications Of Waves');
  conceptDesc = conceptDesc.replace(/(Enviornment)/ig, 'Environment');
  conceptDesc = conceptDesc.replace(/(Environmnet)/ig, 'Environment');
  conceptDesc = conceptDesc.replace(/(Force, & Motion)/ig, 'Force & Motion');
  conceptDesc = conceptDesc.replace(/(, & )/ig, ' & ');
  conceptDesc = conceptDesc.replace(/(\bOrganismsof\b)/ig, 'Organism of');
  conceptDesc = conceptDesc.replace(/\b(W \/)/ig, 'With');
  conceptDesc = conceptDesc.replace(/(Language \/ Operations)/ig, 'Language & Operations');
  conceptDesc = conceptDesc.replace(/(& Common Organism Of)/ig, '& Common Organisms Of');
  conceptDesc = conceptDesc.replace(/(Organisms Of Utah Environments)/ig, 'Organisms Of Utah\'s Environment');
  conceptDesc = conceptDesc.replace(/(Interact With On Another)/ig, 'Interact With One Another');
  conceptDesc = conceptDesc.replace(/(With & Is Altered By Earth Layers|With & Is Altered By Earth\'s Layers)/ig, 'With & Is Altered By The Earth\'s Layers');
  conceptDesc = conceptDesc.replace(/(Affect Living Systems, Earth Is Unique)/ig, 'Affect Living Systems & Earth Is Unique');
  conceptDesc = conceptDesc.replace(/(Axiz)/ig, 'Axis');
  conceptDesc = conceptDesc.replace(/(Earth\'s Revolving Environment Affect)/ig, 'Earth\'s Evolving Environment Affect');
  conceptDesc = conceptDesc.replace(/(Earth\'s Plates & Causes Plates)/ig, 'Earth\'s Plates & Causes The Plates');
  conceptDesc = conceptDesc.replace(/(As It Revolves Aroung The Sun)/ig, 'As It Revolves Around The Sun');
  conceptDesc = conceptDesc.replace(/(Offsping)/ig, 'Offspring');
  conceptDesc = conceptDesc.replace(/(Cells That Have Structures)/ig, 'Cells That Have Structure');
  conceptDesc = conceptDesc.replace(/(Heat Light & Sound)/ig, 'Heat, Light & Sound');
  conceptDesc = conceptDesc.replace(/(Nature Of Changes In Matter)/ig, 'Nature Of Change In Matter');
  conceptDesc = conceptDesc.replace(/(Relationshi)/ig, 'Relationships');
  conceptDesc = conceptDesc.replace(/(Effetively)/ig, 'Effectively');
  conceptDesc = conceptDesc.replace(/(Diveristy)/ig, 'Diversity');
  conceptDesc = conceptDesc.replace(/(Relationshipsp)/ig, 'Relationship');
  conceptDesc = conceptDesc.replace(/(Sciene)/ig, 'Science');
  conceptDesc = conceptDesc.replace(/(Demostrate)/ig, 'Demonstrate');
  conceptDesc = conceptDesc.replace(/(Relationshipsps)/ig, 'Relationships');
  conceptDesc = conceptDesc.replace(/(Albegra)/ig, 'Algebra');
  conceptDesc = conceptDesc.replace(/(Nterpret)/ig, 'Interpret');
  conceptDesc = conceptDesc.replace(/(Ocabulary)/ig, 'Vocabulary');
  conceptDesc = conceptDesc.replace(/(Vvocabulary)/ig, 'Vocabulary');
  conceptDesc = conceptDesc.replace(/(Determine Area & Surface Area Polygons)/ig, 'Determine Area Of Polygons & Surface Area');
  conceptDesc = conceptDesc.replace(/(Factors Determining Strength Of Gravitational)/ig, 'Factors Determining The Strength Of Gravitational');
  conceptDesc = conceptDesc.replace(/(Iinterpret)/ig, 'Interpret');
  conceptDesc = conceptDesc.replace(/(Offspring Inherit Traits That Affect Survival In The Environment)/ig, 'Offspring Inherit Traits That Make Them More Or Less Suitable To Survive In The Environment');
  conceptDesc = conceptDesc.replace(/(Offspring Inherit Traits That Affect Survival In The Environment)/ig, 'Offspring Inherit Traits That Make Them More Or Less Suitable To Survive In The Environment');
  conceptDesc = conceptDesc.replace(/(Organisms Are Composed Of 1 Or More Cells That Are Made Of Molecules...& Perform Life Functions)/ig, 'Organisms Are Composed Of One Or More Cells That Are Made Of Molecules & Perform Life Functions');
  conceptDesc = conceptDesc.replace(/(Organisms Are Composed Of One Or More Cells That Are Made)/ig, 'Organisms Are Composed Of One Or More Cells That Are Made Of Molecules & Perform Life Functions');
  conceptDesc = conceptDesc.replace(/(Organs In An Organism Are Made Of Cells That Perform Life Functions)/ig, 'Organs In An Organism Are Made Of Cells That Have Structure & Perform Specific Life Functions');
  conceptDesc = conceptDesc.replace(/^(Properties & Behavior Of Heat, Light & Sound)$/ig, 'Understand Properties & Behavior Of Heat, Light & Sound');
  conceptDesc = conceptDesc.replace(/^(Awareness Of Social & Historical Aspects Of Science)$/ig, 'Demonstrate Awareness Of Social & Historical Aspects Of Science');

  // Convert to case where all words start with capital letter
  conceptDesc = conceptDesc.replace(/\w\S*/g, txt => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase());

  return conceptDesc;
}

export function groupBy(keyExtract, arr) {
  return arr.reduce((result, item) => {
    const key = keyExtract(item);
    if (result.has(key)) {
      result.set(key, [...result.get(key), item]);
    } else {
      result.set(key, [item]);
    }

    return result;
  }, new Map());
}

/**
 * iterate through @param array, find matches in @param groups,
 * and extend each object with new fields from each element in @param array
 * @param  {Map}      groups
 * @param  {array}    array
 * @param  {function} mergeKeyExtract
 * @param  {function} mergeNullCheck
 * @return {Map}
 */
export function mergeGroups(groups, array, mergeKeyExtract, mergeNullCheck) {
  try {
    array.forEach(it => {
      try {
        let hasMatch = true;
        if (mergeNullCheck(it)) {
          hasMatch = false;
        }
        const mergeKey = mergeKeyExtract(it);
        let group = groups.get(mergeKey);
        if (!group) {
          throw `${mergeKey} not found in groups`;
        }
        if (hasMatch) {
        	group.forEach(item => Object.assign(item, it));
        } else {
          throw `${mergeKeyExtract(it)} not found in PowerSchool`;
        }

      } catch (e) {
        groups.delete(mergeKeyExtract(it));
        logErrors(it, e, e);
      }
    });
    return groups;
  } catch (e) {
    console.log('merge error');
    console.log(e);
    console.log(array);
  }
}

export function flatten(groups) {
  let arr = []
  for (const [index, group] of groups.entries()) {
    arr.push(...group);
  }
  return arr;
}

/**
 * converts a school year in the format "2011" to "2010-2011"
 * @param  {string|number} shortSchoolYear
 * @return {string}
 */
export function toFullSchoolYear(shortSchoolYear) {
  return `${shortSchoolYear - 1}-${shortSchoolYear}`;
}
