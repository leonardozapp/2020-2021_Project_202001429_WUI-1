<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * CadastroFundos Model
 *
 * @property \App\Model\Table\CnpjFundosTable&\Cake\ORM\Association\BelongsTo $CnpjFundos
 * @property \App\Model\Table\TipoClasseFundosTable&\Cake\ORM\Association\BelongsTo $TipoClasseFundos
 * @property \App\Model\Table\TipoRentabilidadeFundosTable&\Cake\ORM\Association\BelongsTo $TipoRentabilidadeFundos
 * @property \App\Model\Table\GestorFundosTable&\Cake\ORM\Association\BelongsTo $GestorFundos
 * @property \App\Model\Table\AdministradorFundosTable&\Cake\ORM\Association\BelongsTo $AdministradorFundos
 *
 * @method \App\Model\Entity\CadastroFundo newEmptyEntity()
 * @method \App\Model\Entity\CadastroFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\CadastroFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\CadastroFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\CadastroFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\CadastroFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\CadastroFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\CadastroFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\CadastroFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\CadastroFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\CadastroFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\CadastroFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\CadastroFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class CadastroFundosTable extends Table
{
    /**
     * Initialize method
     *
     * @param array $config The configuration for the Table.
     * @return void
     */
    public function initialize(array $config): void
    {
        parent::initialize($config);

        $this->setTable('cadastro_fundos');
        $this->setDisplayField('cnpj_fundo_id');
        $this->setPrimaryKey(['cnpj_fundo_id', 'tipo_classe_fundo_id', 'DT_INI_CLASSE', 'tipo_rentabilidade_fundo_id', 'CONDOM_ABERTO', 'FUNDO_COTAS', 'FUNDO_EXCLUSIVO', 'INVEST_QUALIF', 'gestor_fundo_id', 'administrador_fundo_id']);

        $this->belongsTo('CnpjFundos', [
            'foreignKey' => 'cnpj_fundo_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('TipoClasseFundos', [
            'foreignKey' => 'tipo_classe_fundo_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('TipoRentabilidadeFundos', [
            'foreignKey' => 'tipo_rentabilidade_fundo_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('GestorFundos', [
            'foreignKey' => 'gestor_fundo_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('AdministradorFundos', [
            'foreignKey' => 'administrador_fundo_id',
            'joinType' => 'INNER',
        ]);
    }

    /**
     * Default validation rules.
     *
     * @param \Cake\Validation\Validator $validator Validator instance.
     * @return \Cake\Validation\Validator
     */
    public function validationDefault(Validator $validator): Validator
    {
        $validator
            ->date('DT_CONST')
            ->requirePresence('DT_CONST', 'create')
            ->notEmptyDate('DT_CONST');

        $validator
            ->date('DT_INI_CLASSE')
            ->allowEmptyDate('DT_INI_CLASSE', null, 'create');

        $validator
            ->boolean('CONDOM_ABERTO')
            ->allowEmptyString('CONDOM_ABERTO', null, 'create');

        $validator
            ->boolean('FUNDO_COTAS')
            ->allowEmptyString('FUNDO_COTAS', null, 'create');

        $validator
            ->boolean('FUNDO_EXCLUSIVO')
            ->allowEmptyString('FUNDO_EXCLUSIVO', null, 'create');

        $validator
            ->boolean('INVEST_QUALIF')
            ->allowEmptyString('INVEST_QUALIF', null, 'create');

        $validator
            ->date('DT_REG_CVM')
            ->requirePresence('DT_REG_CVM', 'create')
            ->notEmptyDate('DT_REG_CVM');

        return $validator;
    }

    /**
     * Returns a rules checker object that will be used for validating
     * application integrity.
     *
     * @param \Cake\ORM\RulesChecker $rules The rules object to be modified.
     * @return \Cake\ORM\RulesChecker
     */
    public function buildRules(RulesChecker $rules): RulesChecker
    {
        $rules->add($rules->existsIn(['cnpj_fundo_id'], 'CnpjFundos'));
        $rules->add($rules->existsIn(['tipo_classe_fundo_id'], 'TipoClasseFundos'));
        $rules->add($rules->existsIn(['tipo_rentabilidade_fundo_id'], 'TipoRentabilidadeFundos'));
        $rules->add($rules->existsIn(['gestor_fundo_id'], 'GestorFundos'));
        $rules->add($rules->existsIn(['administrador_fundo_id'], 'AdministradorFundos'));

        return $rules;
    }
}
