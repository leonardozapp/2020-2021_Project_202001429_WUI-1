<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * CnpjFundos Model
 *
 * @property \App\Model\Table\CadastroFundosTable&\Cake\ORM\Association\HasMany $CadastroFundos
 * @property \App\Model\Table\CancelamentoFundosTable&\Cake\ORM\Association\HasMany $CancelamentoFundos
 * @property \App\Model\Table\DocExtratosFundosTable&\Cake\ORM\Association\HasMany $DocExtratosFundos
 * @property \App\Model\Table\DocInfDiarioFundosTable&\Cake\ORM\Association\HasMany $DocInfDiarioFundos
 * @property \App\Model\Table\IndicadoresFundosTable&\Cake\ORM\Association\HasMany $IndicadoresFundos
 * @property \App\Model\Table\OperacoesFinanceirasTable&\Cake\ORM\Association\HasMany $OperacoesFinanceiras
 * @property \App\Model\Table\SituacaoFundosTable&\Cake\ORM\Association\HasMany $SituacaoFundos
 *
 * @method \App\Model\Entity\CnpjFundo newEmptyEntity()
 * @method \App\Model\Entity\CnpjFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\CnpjFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\CnpjFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\CnpjFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\CnpjFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\CnpjFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\CnpjFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\CnpjFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\CnpjFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\CnpjFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\CnpjFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\CnpjFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class CnpjFundosTable extends Table
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

        $this->setTable('cnpj_fundos');
        $this->setDisplayField('id');
        $this->setPrimaryKey('id');

        $this->hasMany('CadastroFundos', [
            'foreignKey' => 'cnpj_fundo_id',
        ]);
        $this->hasMany('CancelamentoFundos', [
            'foreignKey' => 'cnpj_fundo_id',
        ]);
        $this->hasMany('DocExtratosFundos', [
            'foreignKey' => 'cnpj_fundo_id',
        ]);
        $this->hasMany('DocInfDiarioFundos', [
            'foreignKey' => 'cnpj_fundo_id',
        ]);
        $this->hasMany('IndicadoresFundos', [
            'foreignKey' => 'cnpj_fundo_id',
        ]);
        $this->hasMany('OperacoesFinanceiras', [
            'foreignKey' => 'cnpj_fundo_id',
        ]);
        $this->hasMany('SituacaoFundos', [
            'foreignKey' => 'cnpj_fundo_id',
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
            ->integer('id')
            ->allowEmptyString('id', null, 'create');

        $validator
            ->scalar('CNPJ')
            ->maxLength('CNPJ', 20)
            ->requirePresence('CNPJ', 'create')
            ->notEmptyString('CNPJ')
            ->add('CNPJ', 'unique', ['rule' => 'validateUnique', 'provider' => 'table']);

        $validator
            ->scalar('DENOM_SOCIAL')
            ->maxLength('DENOM_SOCIAL', 100)
            ->requirePresence('DENOM_SOCIAL', 'create')
            ->notEmptyString('DENOM_SOCIAL');

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
        $rules->add($rules->isUnique(['CNPJ']));

        return $rules;
    }
}
