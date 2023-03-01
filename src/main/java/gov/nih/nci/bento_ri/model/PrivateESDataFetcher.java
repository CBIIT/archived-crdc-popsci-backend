package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.search.MultipleRequests;
import gov.nih.nci.bento.model.search.filter.DefaultFilter;
import gov.nih.nci.bento.model.search.filter.FilterParam;
import gov.nih.nci.bento.model.search.mapper.TypeMapperImpl;
import gov.nih.nci.bento.model.search.mapper.TypeMapperService;
import gov.nih.nci.bento.model.search.query.QueryParam;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
import graphql.schema.idl.RuntimeWiring;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Request;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

@Component
public class PrivateESDataFetcher extends AbstractPrivateESDataFetcher {
    private static final Logger logger = LogManager.getLogger(PrivateESDataFetcher.class);
    private final YamlQueryFactory yamlQueryFactory;
    private final TypeMapperService typeMapper = new TypeMapperImpl();

    public PrivateESDataFetcher(ESService esService) {
        super(esService);
        yamlQueryFactory = new YamlQueryFactory(esService);
    }

    @Override
    public RuntimeWiring buildRuntimeWiring() throws IOException {
        return RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("QueryType")
                        .dataFetchers(yamlQueryFactory.createYamlQueries(Const.ES_ACCESS_TYPE.PRIVATE))
                        .dataFetcher("studyInfo", env -> studyInfo())
                )
                .build();
    }

    private List<Map<String, Object>> studyInfo() throws IOException {
        /*
            Properties definition template:
            String[][] properties = new String[][]{
                new String[]{ <Return Label>, <ES Index property name>}
            };
        */
        String[][] properties = new String[][]{
            new String[]{"cases", "num_cases"},
            new String[]{"program", "program"},
            new String[]{"study_code", "study_code"},
            new String[]{"study_id", "study_id"},
            new String[]{"study_name", "study_name"},
            new String[]{"study_type", "study_type"}
        };
        //Generic Query
        Map<String, Object> query = esService.buildListQuery();
        Request request = new Request("GET", STUDIES_END_POINT);
        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
    }
}
