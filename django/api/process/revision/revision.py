class RevisionProcess:
    def __init__(self, cea, cpa):
        self._cea = cea
        self._cpa = cpa

    def compute(self):
        # CEA revision
        row_results = []
        for _, (subjects, linkages) in enumerate(self._cea):              
            winning_row_subject = None
            winning_row_linkages = []
            winning_row_confidence = 0.0
                
            # Take subjects with maximum score, leave the rest. This is for performance reasons
            subject_items = sorted(
                subjects.items(),   # (subject, confidence)
                key=lambda item: item[1],
                reverse=True
            )
            max_subject_confidence = 0.0
            if len(subject_items) > 0:
                max_subject_confidence = subject_items[0][1]
            """ TODO: default is bugged??
            max_subject_confidence = max(
                subject_items,
                key=lambda item: item[1],
                default=[None, 0.0]
            )[1]
            """
            
            # Get all subjects with max confidence (could be more than one)
            """
            most_confident_subjects_items = [
                item
                for item in subject_items
                if item[1] == max_subject_confidence
            ]
            """
            # TODO: Bypass performance euristic (all stuff before this line)
            most_confident_subjects_items = subject_items

            for winning_subject, s_confidence in most_confident_subjects_items:
                if winning_subject is not None:
                    winning_triples = []
                    for col, linkage in enumerate(linkages):
                        winning_links = []
                            
                        for link in linkage:
                            if link.subject() == winning_subject:
                                # Revise cea with cpa
                                if len(self._cpa[col]) > 0:
                                    if link.predicate() in self._cpa[col]:
                                        winning_links.append((
                                            link.get_triple(),  # Original triple
                                            self._cpa[col][link.predicate()] * link.get_confidence()    # Predicate confidence AND link confidence
                                        ))
                        
                        # Now take the most confident link from all candidates
                        winning_triples.append(
                            max (
                                winning_links,
                                key=lambda item: item[1],
                                default=(None, 0)
                            )
                        )
                            
                    # subj_conf * (obj1_conf + obj2_conf + ... + objn_conf)
                    row_confidence = s_confidence * sum([
                        wt[1]
                        for wt in winning_triples
                    ])
                    # Set winning row subject, linkages and confidence
                    if row_confidence > winning_row_confidence:
                        winning_row_subject = winning_subject
                        winning_row_linkages = winning_triples
                        winning_row_confidence = row_confidence

            row_results.append((winning_row_subject, winning_row_linkages, winning_row_confidence))

        return row_results
